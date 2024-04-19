package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
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
        this.topicHandlerFunctions = new HashMap<>();
    }

    String getMyAddr() {
        return myAddr;
    }

    private Map<String, Method> annotationListeners;
    private Map<String, Method> scanActionListeners(Object handlerInstance) {
        return Arrays.stream(handlerInstance.getClass().getMethods())
                .filter(method -> method.isAnnotationPresent(ActorListener.class) || method.isAnnotationPresent(ActorScheduler.class))
                .collect(Collectors.toMap(this::methodToTopic, method -> method));
    }

    private String methodToTopic(Method method) {
        String topic = method.isAnnotationPresent(ActorListener.class) ?
                method.getAnnotation(ActorListener.class).topic() :
                method.getAnnotation(ActorScheduler.class).topic();
        if (topic.isEmpty()) {
            topic = method.getName();
        }
        return topic;
    }
    private List<ActorScheduler> schedulers;
    private List<ActorScheduler> scanSchedulers(Object handlerInstance) {
        return Arrays.stream(handlerInstance.getClass().getMethods())
                .filter(method -> method.isAnnotationPresent(ActorScheduler.class))
                .map(method -> method.getAnnotation(ActorScheduler.class))
                .collect(Collectors.toList());
    }

    List<ActorScheduler> getSchedulers() {
        return schedulers;
    }

    <T> void addTopicHandlerFunction(String topic, Consumer<T> consumer) {
        try {
            addTopicHandlerFunction(topic, consumer, consumer.getClass().getMethod("accept", Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    <T, U> void addTopicHandlerFunction(String topic, BiConsumer<T, U> consumer) {
        try {
            addTopicHandlerFunction(topic, consumer, consumer.getClass().getMethod("accept", Object.class, Object.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    void addTopicHandlerFunction(String topic, Object instance, Method method) {
        topicHandlerFunctions.put(topic, new Tuple<>(instance, method));
    }

    void setDefaultHandlerFunction(Consumer<ActorMsg> handlerFunction) {
        this.defaultHandlerFunction = handlerFunction;
    }

    void setHandlerInstance(Object handlerInstance) {
        this.handlerInstance = handlerInstance;
        this.annotationListeners = scanActionListeners(handlerInstance);
        this.schedulers = scanSchedulers(handlerInstance);
    }

    Set<String> getSubscribedTopics() {
        return annotationListeners.entrySet().stream()
                .filter(entry -> entry.getValue().getAnnotation(ActorListener.class).consumer())
                .filter(entry -> entry.getValue().getParameterCount() <= 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private boolean tryInvoke(Tuple<Object, Method> function, ActorMsg msg) throws InvocationTargetException, IllegalAccessException {
        if (function != null) {
            Object instance = function.first();
            Method method = function.second();
            if (method.getParameterCount() == 1 && ActorMsg.class.equals(method.getParameters()[0].getType()) && !method.getParameters()[0].isAnnotationPresent(Payload.class)) {
                // 优先看参数个数是否是1，且参数类型是ActorMsg，且没有@Payload注解
                method.invoke(instance, msg);
                return true;
            } else {
                if (method.getParameterCount() == msg.getPayloads().length) {
                    method.invoke(instance, msg.getPayloads());
                    return true;
                } else if (method.getParameterCount() < msg.getPayloads().length) {
                    // 允许接受消息的方法参数个数比消息的参数个数少，忽略后面的参数
                    method.invoke(instance, Arrays.copyOf(msg.getPayloads(), method.getParameterCount()));
                    return true;
                }
            }
        }
        return false;
    }

    private void invokeAnnotationListener(ActorMsg msg, Method method) throws InvocationTargetException, IllegalAccessException {
        if (method.isAnnotationPresent(ActorScheduler.class)) {
            method.invoke(handlerInstance);
        } else {
            Object ret = null;
            Throwable throwable = null;
            try {
                if (method.getParameterCount() == 0) {
                    ret = method.invoke(handlerInstance);
                } else {
                    if (method.getParameterCount() == 1 && ActorMsg.class.equals(method.getParameters()[0].getType()) && !method.getParameters()[0].isAnnotationPresent(Payload.class)) {
                        // 优先看参数个数是否是1，且参数类型是ActorMsg，且没有@Payload注解
                        ret = method.invoke(handlerInstance, msg);
                    } else {
                        if (method.getParameterCount() == msg.getPayloads().length) {
                            ret = method.invoke(handlerInstance, msg.getPayloads());
                        } else if (method.getParameterCount() < msg.getPayloads().length) {
                            // 允许接受消息的方法参数个数比消息的参数个数少，忽略后面的参数
                            ret = method.invoke(handlerInstance, Arrays.copyOf(msg.getPayloads(), method.getParameterCount()));
                        }
                    }
                }
            } catch (Throwable t) {
                throwable = t;
                throw t;
            } finally {
                if (method.getAnnotation(ActorListener.class).response()) {
                    if (throwable != null) {
                        this.outbox.send(msg.getSender(), RESPONSE, new ActorResponse(msg.getTopic(), msg.getSequentialId(), throwable));
                    } else {
                        this.outbox.send(msg.getSender(), RESPONSE, new ActorResponse(msg.getTopic(), msg.getSequentialId(), ret));
                    }
                }
            }


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
                    Method method = Arrays.stream(this.handlerInstance.getClass().getMethods())
                            .filter(m -> m.getName().equals(msg.getTopic()))
                            .filter(m -> (m.getParameterCount() == 1 && ActorMsg.class.equals(m.getParameters()[0].getType())) || m.getParameterCount() == msg.getPayloads().length)
                            .max((m1, m2) -> m2.getParameterCount() - m1.getParameterCount())
                            .orElse(null);

                    if (method != null && tryInvoke(new Tuple<>(handlerInstance, method), msg)) {
                        return true;
                    }
                }

                // 默认方法
                if (null != defaultHandlerFunction) {
                    defaultHandlerFunction.accept(msg);
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
}
