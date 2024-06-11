package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.Tuple;
import io.journalkeeper.utils.actor.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.*;
import java.util.stream.Collectors;

import static io.journalkeeper.utils.actor.ActorResponseSupport.RESPONSE;

class ActorInbox {
    private static final Logger logger = LoggerFactory.getLogger( ActorInbox.class );
    // 收件箱队列
    private final Queue<ActorMsg> msgQueue;
    // 显式注册的收消息方法
    private final Map<String /* topic */, Tuple<Object /* instance */, Method>> topicHandlerFunctions;
    // 兜底处理所有未被处理消息的方法
    private Consumer<ActorMsg> defaultHandlerFunction;
    // 收件箱地址
    private final String myAddr;
    // 收消息的实例对象
    private Object handlerInstance;
    // 默认收件箱容量
    final static int DEFAULT_CAPACITY = 1000;

    private final ActorOutbox outbox;

    // 收到消息后，通知邮递员派送消息的响铃
    private Object ring;

    ActorInbox(String myAddr, ActorOutbox outbox) {
        this(DEFAULT_CAPACITY, myAddr, outbox);
    }

    ActorInbox(int capacity, String myAddr, ActorOutbox outbox) {
        this.myAddr = myAddr;
        msgQueue = new LinkedBlockingQueue<>(capacity);
        this.outbox = outbox;
        this.defaultHandlerFunction = null;
        this.topicHandlerFunctions = new ConcurrentHashMap<>();
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
                    this.outbox.send(msg.getSender(), RESPONSE, new ActorResponse(msg, ret));
                }
            } catch (InvocationTargetException ite) {
                if (needResponse(msg, method)) {
                    this.outbox.send(msg.getSender(), RESPONSE, new ActorResponse(msg, ite.getCause()));
                }
                logger.info("Invoke message handler exception, handler: {}, msg: {}, exception: ", instance.getClass().getName() + "." + method.getName() + "(...)", msg, ite.getCause());
            } catch (IllegalArgumentException e) {
                if (needResponse(msg, method)) {
                    this.outbox.send(msg.getSender(), RESPONSE, new ActorResponse(msg, e));
                }
                logger.info("Invoke message handler exception, handler: {}, msg: {}, exception: {}.", instance.getClass().getName() + "." + method.getName() + "(...)", msg, e.toString());

            }
            return true;
        }
        return false;
    }

    private boolean needResponse(ActorMsg msg, Method method) {
        return (msg.getResponse() == ActorMsg.Response.REQUIRED
                || (msg.getResponse() == ActorMsg.Response.DEFAULT && !void.class.equals(method.getReturnType()))) && !method.isAnnotationPresent(ResponseManually.class);
    }

    private void invokeAnnotationListener(ActorMsg msg, Method method) throws IllegalAccessException {
        if (method.isAnnotationPresent(ActorScheduler.class)) {
            try {
                method.setAccessible(true);
                method.invoke(handlerInstance);
            } catch (InvocationTargetException ite) {
                if (!void.class.equals(method.getReturnType())) {
                    this.outbox.send(msg.getSender(), RESPONSE, new ActorResponse(msg, ite.getCause()));
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
    boolean processOneMsg(){
        ActorMsg msg = msgQueue.poll();
        if (msg != null) {
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
            }

            return true;
        }
        return false;
    }



    void receive(ActorMsg msg) {
        msgQueue.add(msg);
        ring();
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
