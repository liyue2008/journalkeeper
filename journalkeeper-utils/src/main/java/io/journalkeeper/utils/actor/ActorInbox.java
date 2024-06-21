package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.Tuple;
import io.journalkeeper.utils.actor.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.*;
import java.util.stream.Collectors;

class ActorInbox {
    private static final Logger logger = LoggerFactory.getLogger( ActorInbox.class );
    // 收件箱队列
    private final BlockingQueue<ActorMsg> msgQueue;
    // 显式注册的收消息方法
    private final Map<String /* topic */, Tuple<Object /* instance */, Method>> topicHandlerFunctions;
    // 兜底处理所有未被处理消息的方法
    private Consumer<ActorMsg> defaultHandlerFunction;
    // 收件箱地址
    private final String myAddr;
    // 收消息的实例对象
    private Object handlerInstance;
    // 默认收件箱容量
    final static int DEFAULT_CAPACITY = Integer.MAX_VALUE;

    private final ActorOutbox outbox;

    private final Map<String, BlockingQueue<ActorMsg>> topicQueueMap;
    private final List<BlockingQueue<ActorMsg>> allMsgQueues;

    // 收到消息后，通知邮递员派送消息的响铃
    private Object ring;

    ActorInbox(int capacity, String myAddr, Map<String, Integer> topicQueueMap, ActorOutbox outbox) {
        this.myAddr = myAddr;
        msgQueue = new LinkedBlockingQueue<>(capacity < 0 ? DEFAULT_CAPACITY : capacity);
        this.outbox = outbox;
        this.defaultHandlerFunction = null;
        this.topicHandlerFunctions = new ConcurrentHashMap<>();
        this.topicQueueMap = new HashMap<>();
        if (null != topicQueueMap) {
            for (Map.Entry<String, Integer> entry : topicQueueMap.entrySet()) {
                this.topicQueueMap.put(entry.getKey(), new LinkedBlockingQueue<>(entry.getValue() < 0 ? DEFAULT_CAPACITY : entry.getValue()));
            }
        }

        allMsgQueues = new ArrayList<>();
        allMsgQueues.add(msgQueue);
        for (Map.Entry<String, BlockingQueue<ActorMsg>> entry : this.topicQueueMap.entrySet()) {
            allMsgQueues.add(entry.getValue());
        }
    }

    String getMyAddr() {
        return myAddr;
    }

    private Map<String, Method> annotationListeners = new HashMap<>();

    private List<ScheduleTask> schedulers = new LinkedList<>();
    private List<ScheduleTask> scanSchedulers(Object handlerInstance) {
        return Arrays.stream(handlerInstance.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(ActorScheduler.class))
                .map(method -> new ScheduleTask(getMyAddr(), ActorUtils.methodToTopic(method, ActorScheduler.class), method.getAnnotation(ActorScheduler.class)))
                .collect(Collectors.toList());
    }

    List<ScheduleTask> getSchedulers() {
        return schedulers;
    }


    <R> void addTopicHandlerFunction(String topic, Supplier<R> handler) {
        try {
            addTopicHandlerFunction(topic, handler, handler.getClass().getDeclaredMethod("get"));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
    <T, R> void addTopicHandlerFunction(String topic, Function<T, R> handler) {
        try {
            addTopicHandlerFunction(topic, handler, handler.getClass().getDeclaredMethod("apply", Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    <T, U, R> void addTopicHandlerFunction(String topic, BiFunction<T, U, R> handler) {
        try {
            addTopicHandlerFunction(topic, handler, handler.getClass().getDeclaredMethod("apply", Object.class, Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    void addTopicHandlerFunction(String topic, Runnable runnable) {
        try {
            addTopicHandlerFunction(topic, runnable, runnable.getClass().getDeclaredMethod("run"));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
    <T> void addTopicHandlerFunction(String topic, Consumer<T> consumer) {
        try {
            addTopicHandlerFunction(topic, consumer, consumer.getClass().getDeclaredMethod("accept", Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    <T, U> void addTopicHandlerFunction(String topic, BiConsumer<T, U> consumer) {
        try {
            addTopicHandlerFunction(topic, consumer, consumer.getClass().getDeclaredMethod("accept", Object.class, Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    void addTopicHandlerFunction(String topic, Object instance, Method method) {
        topicHandlerFunctions.put(topic, new Tuple<>(instance, method));
    }

    void removeTopicHandlerFunction(String topic) {
        topicHandlerFunctions.remove(topic);
    }

    void setDefaultHandlerFunction(Consumer<ActorMsg> handlerFunction) {
        this.defaultHandlerFunction = handlerFunction;
    }

    void setHandlerInstance(Object handlerInstance) {
        this.handlerInstance = handlerInstance;
        this.annotationListeners = ActorUtils.scanActionListeners(handlerInstance, Arrays.asList(ActorSubscriber.class, ActorListener.class, ActorScheduler.class));
        this.schedulers = scanSchedulers(handlerInstance);
    }

    Set<String> getSubscribedTopics() {
        return annotationListeners.entrySet().stream()
                .filter(entry -> entry.getValue().isAnnotationPresent(ActorSubscriber.class))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private boolean tryInvoke(Tuple<Object, Method> function, ActorMsg msg) throws IllegalAccessException {
        if (function != null) {
            Object instance = function.first();
            Method method = function.second();
            Object ret;
            try {
                if (method.getParameterCount() == 1 && method.getParameters()[0].isAnnotationPresent(ActorMessage.class)) {
                    // 优先看参数个数是否是1，且带有@ActorMessage注解
                    method.setAccessible(true);
                    ret = method.invoke(instance, msg);
                } else {
                    method.setAccessible(true);
                    ret = method.invoke(instance, msg.getPayloads());
                }
                if (needResponse(msg, method)) {
                    this.outbox.send(this.outbox.createResponse(msg, ret, null));
                }
            } catch (InvocationTargetException ite) {
                if (needResponse(msg, method)) {
                    this.outbox.send(this.outbox.createResponse(msg, null, ite.getCause()));

                }
                logger.info("Invoke message handler exception, handler: {}, msg: {}, exception: ", instance.getClass().getName() + "." + method.getName() + "(...)", msg, ite.getCause());
            } catch (IllegalArgumentException e) {
                if (needResponse(msg, method)) {
                    this.outbox.send(this.outbox.createResponse(msg, null, e));

                }
                logger.info("Invoke message handler failed, cause: illegal argument, handler: {}, msg: {}.", instance.getClass().getName() + "." + method.getName() + "(...)", msg);

            }
            return true;
        }
        return false;
    }

    private boolean needResponse(ActorMsg msg, Method method) {
        return (msg.getContext().getResponseConfig() == ActorMsg.Response.REQUIRED
                || (msg.getContext().getResponseConfig() == ActorMsg.Response.DEFAULT && !void.class.equals(method.getReturnType()))) && !method.isAnnotationPresent(ResponseManually.class);
    }

    private void invokeAnnotationListener(ActorMsg msg, Method method) throws IllegalAccessException {
        if (method.isAnnotationPresent(ActorScheduler.class)) {
            try {
                method.setAccessible(true);
                method.invoke(handlerInstance);
            } catch (InvocationTargetException ite) {
                if (!void.class.equals(method.getReturnType())) {
                    this.outbox.send(this.outbox.createResponse(msg, null, ite.getCause()));
                }
                logger.info("Invoke message handler exception, handler: {}, msg: {}, exception: {}.", handlerInstance.getClass().getName() + "." + method.getName() + "(...)", msg, ite.getCause().toString());
            }
        } else {
            tryInvoke(new Tuple<>(handlerInstance, method), msg);
        }
    }

    /**
     * 处理一个消息.
     * 从收件箱中获取一个消息，然后寻找并调用对应的接收消息方法.
     * 每个消息只处理一次，无论消息处理是否成功。
     * 如果有多个处理方法都匹配主题，按如下优先级调用第一个收消息方法：
     * 1. 通过addTopicHandlerFunction显式注册的；
     * 如果通过setHandlerInstance设置了收消息的对象：
     * 2. 通过注解注册的；
     * 3. topic 同名方法；
     * 4. 通过setDefaultHandlerFunction设置的默认收消息方法。
     * @return 是否处理了一个消息。
     *  true：收件箱里有消息，且处理了一个消息，无论处理成功与否，都返回true；
     *  false：收件箱里没有消息，返回false。
     */
    boolean processOneMsg() {
        boolean hasMessage = false;
        for (BlockingQueue<ActorMsg> queue : allMsgQueues) {
            if (processOneMsgFromQueue(queue)) {
                hasMessage = true;
            }
        }
        return hasMessage;
    }

    private static final ThreadLocal<SimpleDateFormat> sdfHolder
            = ThreadLocal.withInitial(() -> new SimpleDateFormat("HH:mm:ss.SSS"));

    private boolean processOneMsgFromQueue(BlockingQueue<ActorMsg> queue){
        ActorMsg msg = queue.poll();
        if (msg != null) {
            if (msg.getContext().getMetric() != null) {
                msg.getContext().getMetric().onDeliver();
            }
            try {
                if(msg.getTopic().startsWith("@")) {
                    String methodName = msg.getTopic().substring(1);
                    if ("addTopicHandlerFunction".equals(methodName)) {
                        addTopicHandlerFunction(msg.getPayload(), msg.<Runnable>getPayload(1));
                    } else if ("removeTopicHandlerFunction".equals(methodName)) {
                        removeTopicHandlerFunction(msg.getPayload());
                    }else {
                        throw new IllegalArgumentException("Unsupported method: " + methodName);
                    }
                    return true;
                }

                // 显式注册的方法
                Tuple<Object, Method> tuple = topicHandlerFunctions.get(msg.getTopic());
                if (tryInvoke(tuple, msg)){
                    return true;
                }
                if (null != handlerInstance) {

                    if (null != annotationListeners && annotationListeners.containsKey(msg.getTopic())){
                        // 通过注解注册的方法
                        Method method = annotationListeners.get(msg.getTopic());
                        invokeAnnotationListener(msg, method);
                        return true;
                    }

                    // topic 同名方法
                    Class<?> [] payloadTypes = Arrays.stream(msg.getPayloads()).map(Object::getClass).toArray(Class<?>[]::new);
                    Method method;
                    try {
                        // 尝试精确获取
                        method = this.handlerInstance.getClass().getDeclaredMethod(msg.getTopic(), payloadTypes);
                    } catch (NoSuchMethodException ignored) {
                        // 尝试模糊获取：方法名相同，参数个数相同
                        method = Arrays.stream(this.handlerInstance.getClass().getDeclaredMethods())
                                .filter(m -> !m.getDeclaringClass().equals(Object.class))
                                .filter(m -> m.getName().equals(msg.getTopic()))
                                .filter(m -> m.getParameterCount() == payloadTypes.length).findFirst().orElse(null);
                    }
                    if (method != null && tryInvoke(new Tuple<>(handlerInstance, method), msg)) {
                        return true;
                    }
                }

                // 默认方法
                if (null != defaultHandlerFunction) {
                    try {
                        defaultHandlerFunction.accept(msg);
                    } catch (Exception e) {
                        logger.info("Invoke default handler exception, handler: {}, msg: {}, exception: {}.", defaultHandlerFunction.getClass().getName(), msg, e.toString());
                    }
                    return true;
                }
                logger.warn("No handler for msg: {}", msg);
            } catch (Throwable t) {
                logger.warn("Invoke handler exception, msg: {}", msg, t);
            } finally {
                if (msg.getContext().getMetric() != null) {
                    msg.getContext().getMetric().onConsumed();
                    
                    if (msg.getContext().getMetric() != null) {
                        if (msg.getContext().getType() == ActorMsg.Type.REQUEST && msg.getContext().getResponseConfig() != ActorMsg.Response.REQUIRED) {
                            ActorMetric metric = msg.getContext().getMetric();
                            if (metric.cost() > 100L) {
                                logger.info("SlowMsg: {}-(+{}ms)->Outbox({})[{}]-(+{}ms)->Inbox({})[{}]->(+{}ms)->Consumer({}ms)",
                                        sdfHolder.get().format(new Date(metric.getCreateTime())),
                                        metric.getOutboxTime() - metric.getCreateTime(),
                                        metric.getOutBoxQueueName(),
                                        metric.getOutBoxQueueSize(),
                                        metric.getInboxTime() - metric.getOutboxTime(),
                                        metric.getInboxQueueName(),
                                        metric.getInboxQueueSize(),
                                        metric.getDeliverTime() - metric.getInboxTime(),
                                        metric.getCreateTime() - metric.getOutboxTime()
                                );
                            }
                        } else if (msg.getContext().getType() == ActorMsg.Type.RESPONSE){
                            ActorMetric requestMetric = msg.getRequest().getContext().getMetric();
                            ActorMetric responseMetric = msg.getContext().getMetric();
                            if (responseMetric.getConsumedTime() - requestMetric.getCreateTime() > 100L) {
                                logger.info("SlowResponse: {}-(+{}ms)->Outbox({})[{}]-(+{}ms)->Inbox({})[{}]->(+{}ms)->Consumer({}ms)-(+{}ms)->Outbox({})[{}]-(+{}ms)->Inbox({})[{}]->(+{}ms)->Sender({}ms)",
                                        sdfHolder.get().format(new Date(requestMetric.getCreateTime())),
                                        requestMetric.getOutboxTime() - requestMetric.getCreateTime(),
                                        requestMetric.getOutBoxQueueName(),
                                        requestMetric.getOutBoxQueueSize(),
                                        requestMetric.getInboxTime() - requestMetric.getOutboxTime(),
                                        requestMetric.getInboxQueueName(),
                                        requestMetric.getInboxQueueSize(),
                                        requestMetric.getDeliverTime() - requestMetric.getInboxTime(),
                                        requestMetric.getCreateTime() - requestMetric.getOutboxTime(),

                                        responseMetric.getOutboxTime() - requestMetric.getConsumedTime(),
                                        responseMetric.getOutBoxQueueName(),
                                        responseMetric.getOutBoxQueueSize(),
                                        responseMetric.getInboxTime() - responseMetric.getOutboxTime(),
                                        responseMetric.getInboxQueueName(),
                                        responseMetric.getInboxQueueSize(),
                                        responseMetric.getDeliverTime() - responseMetric.getInboxTime(),
                                        responseMetric.getCreateTime() - responseMetric.getOutboxTime()
                                );
                            }
                        }
                    }
                }
            }

            return true;
        }
        return false;
    }



    void receive(ActorMsg msg) {
        BlockingQueue<ActorMsg> queue = topicQueueMap.getOrDefault(msg.getQueueName(), msgQueue);
        queue.add(msg);
        ring();
        if (msg.getContext().getMetric() != null) {
            msg.getContext().getMetric().onInboxIn(msg.getQueueName(), queue.size());
        }
    }

    void setRing(Object ring) {
        this.ring = ring;
    }

    private void ring() {
        if(ring != null) {
            //noinspection SynchronizeOnNonFinalField
            synchronized (ring) {
                ring.notify();
            }
        }
    }

    boolean cleared() {
        return msgQueue.isEmpty();
    }

    public int getQueueSize() {
        return msgQueue.size();
    }
}
