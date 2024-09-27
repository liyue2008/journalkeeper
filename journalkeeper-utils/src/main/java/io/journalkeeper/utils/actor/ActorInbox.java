package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.*;
import java.util.stream.Collectors;

class ActorInbox {
    private static final Logger logger = LoggerFactory.getLogger( ActorInbox.class );
    // 显式注册的收消息方法
    private final Map<String /* topic */, List<InvocationTarget>> actorListeners;

    // 兜底处理所有未被处理消息的方法
    private Consumer<ActorMsg> defaultHandlerFunction;

    // 收件箱地址
    private final String myAddr;
    // 收消息的实例对象
    private Object handlerInstance;

    private final ActorOutbox outbox;

    private final Map<String, BlockingQueue<ActorMsg>> topicQueueMap;

    // 收到消息后，通知邮递员派送消息的响铃
    private Object ring;

    private final int defaultCapacity;

    ActorInbox(int defaultCapacity, String myAddr, Map<String, Integer> topicQueueMap, ActorOutbox outbox) {
        this.defaultCapacity = defaultCapacity;
        this.myAddr = myAddr;
        this.outbox = outbox;
        this.defaultHandlerFunction = null;
        this.actorListeners = new ConcurrentHashMap<>();
        this.topicQueueMap = new ConcurrentHashMap<>();
        if (null != topicQueueMap) {
            for (Map.Entry<String, Integer> entry : topicQueueMap.entrySet()) {
                this.topicQueueMap.put(entry.getKey(), new LinkedBlockingQueue<>(entry.getValue() < 0 ? defaultCapacity : entry.getValue()));
            }
        }
    }

    String getMyAddr() {
        return myAddr;
    }


    List<ScheduleTask> getSchedulers() {
        return this.actorListeners.values().stream().flatMap(Collection::stream)
                .filter(t -> t.getType() == InvocationTarget.TargetType.SCHEDULER)
                .map(t -> new ScheduleTask(t.getTimeUnit(), t.getInterval(), getMyAddr(), t.getTopic())).collect(Collectors.toList());
    }


    <R> void addActorListener(String topic, Supplier<R> handler) {
        try {
            addActorListener(topic, handler, handler.getClass().getDeclaredMethod("get"));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
    <T, R> void addActorListener(String topic, Function<T, R> handler) {
        try {
            addActorListener(topic, handler, handler.getClass().getDeclaredMethod("apply", Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    <T, U, R> void addActorListener(String topic, BiFunction<T, U, R> handler) {
        try {
            addActorListener(topic, handler, handler.getClass().getDeclaredMethod("apply", Object.class, Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    void addActorListener(String topic, Runnable runnable) {
        try {
            addActorListener(topic, runnable, runnable.getClass().getDeclaredMethod("run"));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    <T> void addActorListener(String topic, Consumer<T> consumer, Class<T> tClass) {
        try {
            addActorListener(topic, consumer, consumer.getClass().getDeclaredMethod("accept", tClass));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
    
    <T> void addActorListener(String topic, Consumer<T> consumer) {
        try {
            addActorListener(topic, consumer, consumer.getClass().getDeclaredMethod("accept", Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    <T, U> void addActorListener(String topic, BiConsumer<T, U> consumer) {
        try {
            addActorListener(topic, consumer, consumer.getClass().getDeclaredMethod("accept", Object.class, Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    void addActorListener(String topic, Object instance, Method method) {
        List<InvocationTarget> targets = actorListeners.computeIfAbsent(topic, k -> new LinkedList<>());
        targets.add(new InvocationTarget(instance, method));
    }

    void removeActorListener(String topic, Runnable runnable) {
        List<InvocationTarget> targets = actorListeners.get(topic);
        if (targets == null) {
            return;
        }
        targets.removeIf(target -> {
            try {
                return target.getMethod().equals(runnable.getClass().getDeclaredMethod("run"));
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        });
        if (targets.isEmpty()) {
            actorListeners.remove(topic);
        }
    }



    void addActorSubscriber(String topic, Runnable runnable) {
        try {
            addActorSubscriber(topic, runnable, runnable.getClass().getDeclaredMethod("run"));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
    <T> void addActorSubscriber(String topic, Consumer<T> consumer) {
        try {
            addActorSubscriber(topic, consumer, consumer.getClass().getDeclaredMethod("accept", Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    <T, U> void addActorSubscriber(String topic, BiConsumer<T, U> consumer) {
        try {
            addActorSubscriber(topic, consumer, consumer.getClass().getDeclaredMethod("accept", Object.class, Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
    
    void addActorSubscriber(String topic, Object instance, Method method) {
        List<InvocationTarget> targets = actorListeners.computeIfAbsent(topic, k -> new LinkedList<>());
        targets.add(new InvocationTarget(instance, method, InvocationTarget.TargetType.SUBSCRIBER, topic));
    }

    void setDefaultHandlerFunction(Consumer<ActorMsg> handlerFunction) {
        this.defaultHandlerFunction = handlerFunction;
    }

    void setHandlerInstance(Object handlerInstance) {
        this.handlerInstance = handlerInstance;
        AddAllToActorListener(scanAllMethods());

    }

    private void AddAllToActorListener(Map<String ,List<InvocationTarget>> invocationTargets) {
        ActorUtils.mergeInvocationTargetMap(actorListeners, invocationTargets);
    }

    private Map<String, List<InvocationTarget>> scanAllMethods() {
        return Arrays.stream(this.handlerInstance.getClass().getDeclaredMethods())
                .filter(m -> !m.getDeclaringClass().equals(Object.class))
                .reduce(new HashMap<>(), (map, method) -> {
                    String topic = ActorUtils.methodToTopic(method);
                    List<InvocationTarget> list = map.computeIfAbsent(topic, t -> new LinkedList<>());
                    list.add(new InvocationTarget(handlerInstance, method));
                    return map;
                }, ActorUtils::mergeInvocationTargetMap);
    }

    Set<String> getSubscribedTopics() {
        return actorListeners.entrySet().stream()
                .filter(entry -> entry.getValue().stream().anyMatch(invocationTarget -> invocationTarget.getType().equals(InvocationTarget.TargetType.SUBSCRIBER)))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private void tryInvoke(InvocationTarget invocationTarget, ActorMsg msg) throws IllegalAccessException {
        if (invocationTarget != null) {
            Object instance = invocationTarget.getTarget();
            Method method = invocationTarget.getMethod();
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
                logger.info("Invoke message handler exception, handler: {}, msg: {}, exception: ", instance.getClass().getName() + "." + method.getName() + "(...)", msg, ite.getTargetException());
            } catch (IllegalArgumentException e) {
                if (needResponse(msg, method)) {
                    this.outbox.send(this.outbox.createResponse(msg, null, e));
                }
                logger.info("Invoke message handler failed, cause: illegal argument, handler: {}, msg: {}.", instance.getClass().getName() + "." + method.getName() + "(...)", msg);

            }
        }
    }

    private boolean isMethodSignatureMatch(InvocationTarget invocationTarget, ActorMsg msg) {
        Method method = invocationTarget.getMethod();

        if (method.getParameterCount() == 1 && method.getParameters()[0].isAnnotationPresent(ActorMessage.class) && ClassUtils.isAssignable(ActorMsg.class, method.getParameters()[0].getType())) {
            // 优先看参数个数是否是1，且带有@ActorMessage注解
            return true;
        }

        // 看参数个数和类型是否匹配
        if (method.getParameterCount() != msg.getPayloads().length) {
            return false;
        }
        for (int i = 0; i < msg.getPayloads().length; i++) {
            if (msg.getPayloads()[i] != null &&!ClassUtils.isAssignable(msg.getPayloads()[i].getClass(), method.getParameters()[i].getType())) {
                return false;
            }
        }
        return true;
    }


    private InvocationTarget selectInvocationTarget(List<InvocationTarget> targets, ActorMsg msg) throws InvocationTargetException {
        // 选择一个签名匹配的方法
        if(null == targets) {
            return null;
        }
        List<InvocationTarget> matchTargets = targets.stream().filter(target -> isMethodSignatureMatch(target, msg)).collect(Collectors.toList());
        if (matchTargets.size() > 1) {
            throw new InvocationTargetException(new IllegalStateException("More than one target matched."));
        } else if (matchTargets.size() == 1) {
            return matchTargets.get(0);
        } else {
            return null;
        }
    }

    private boolean needResponse(ActorMsg msg, Method method) {
        return (msg.getContext().getResponseConfig() == ActorMsg.Response.REQUIRED
                || (msg.getContext().getResponseConfig() == ActorMsg.Response.DEFAULT && !void.class.equals(method.getReturnType()))) && !method.isAnnotationPresent(ResponseManually.class);
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
        for (Map.Entry<String, BlockingQueue<ActorMsg>> entry : topicQueueMap.entrySet()) {
            BlockingQueue<ActorMsg> queue = entry.getValue();

            if (processOneMsgFromQueue(queue)) {
                hasMessage = true;
            }

        }
        return hasMessage;
    }

    private boolean processOneMsgFromQueue(BlockingQueue<ActorMsg> queue){
        ActorMsg msg = queue.poll();
        if (msg != null) {
            if (msg.getContext().getMetric() != null) {
                msg.getContext().getMetric().onInboxDequeue(queue.size());
            }
            try {
                if (processInternalMessage(msg)) {
                    return true;
                }

                List<InvocationTarget> targets = actorListeners.get(msg.getTopic());

                // 选择一个签名匹配的方法
                InvocationTarget invocationTarget = selectInvocationTarget(targets, msg);
                if (null != invocationTarget) {
                    tryInvoke(invocationTarget, msg);
                    return true;
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
                }
            }

            return true;
        }
        return false;
    }

    private boolean processInternalMessage(ActorMsg msg) {
        if(msg.getTopic().startsWith("@")) {
            String methodName = msg.getTopic().substring(1);
            switch (methodName) {
                case "addActorListener":
                    addActorListener(msg.getPayload(), msg.<Runnable>getPayload(1));
                    break;
                case "removeActorListener":
                case "removeActorSubscriber":
                    removeActorListener(msg.getPayload(), msg.getPayload(1));
                    break;
                case "addActorSubscriber":
                    addActorSubscriber(msg.getPayload(), msg.<Runnable>getPayload(1));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported method: " + methodName);
            }
            return true;
        }
        return false;
    }


    void receive(ActorMsg msg) {
        BlockingQueue<ActorMsg> queue = topicQueueMap.computeIfAbsent(msg.getQueueName(), queueName -> new LinkedBlockingQueue<>(defaultCapacity));
        queue.add(msg);
        ring();
        if (msg.getContext().getMetric() != null) {
            msg.getContext().getMetric().onInboxEnqueue(msg.getQueueName(), queue.size());
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
        return topicQueueMap.values().stream().allMatch(BlockingQueue::isEmpty);
    }

    public int getQueueSize(String queueName) {
        return topicQueueMap.getOrDefault(queueName, new LinkedBlockingQueue<>(defaultCapacity)).size();
    }

}
