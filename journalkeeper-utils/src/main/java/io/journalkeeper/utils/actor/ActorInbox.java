package io.journalkeeper.utils.actor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;

class ActorInbox {
    private static final Logger logger = LoggerFactory.getLogger( ActorInbox.class );
    private final Queue<ActorMsg> msgQueue;

    private final Map<String /* topic */, Consumer<ActorMsg>> handlers;

    private Consumer<ActorMsg> defaultHandler;

    private final String myAddr;

    private  Object handlerInstance;

    final static int DEFAULT_CAPACITY = 1000;

    private final ActorOutbox outbox;

    ActorInbox(String myAddr, ActorOutbox outbox) {
        this(DEFAULT_CAPACITY, myAddr, outbox);
    }

    ActorInbox(int capacity, String myAddr, ActorOutbox outbox) {
        this.myAddr = myAddr;
        msgQueue = new LinkedBlockingQueue<>(capacity);
        this.outbox = outbox;
        this.defaultHandler = null;
        this.handlers = new HashMap<>();
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

    void addTopicHandlerFunction(String topic, Consumer<ActorMsg> handler) {
        handlers.put(topic, handler);
    }

    void setDefaultHandlerFunction(Consumer<ActorMsg> handler) {
        this.defaultHandler = handler;
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

    boolean processOneMsg(){
        ActorMsg msg = msgQueue.poll();
        if (msg != null) {
            // 显式注册的方法
            Consumer<ActorMsg> consumer = handlers.get(msg.getTopic());
            if (consumer != null) {

                try {
                    consumer.accept(msg);
                } catch (Exception e) {
                    logger.warn("Invoke listener {} failed, msg: {}.", consumer, msg, e);
                }
                return true;

            }
            // 通过注解注册的方法
            if (null != annotationListeners && annotationListeners.containsKey(msg.getTopic())){
                Method method = annotationListeners.get(msg.getTopic());
                try {
                    if (method.isAnnotationPresent(ActorScheduler.class)) {
                        method.invoke(handlerInstance);
                    } else {
                        Object ret;
                        if (method.getParameterCount() == 0) {
                            ret = method.invoke(handlerInstance);
                        } else {

                            if (method.getAnnotation(ActorListener.class).payload()) {
                                ret = method.invoke(handlerInstance, msg.<Object>getPayload());
                            } else {
                                ret = method.invoke(handlerInstance, msg);
                            }
                        }
                        if (method.getAnnotation(ActorListener.class).response()) {
                            outbox.send(msg.getSender(), msg.getTopic(), ret);
                        }

                    }
                } catch (Exception e) {
                    logger.warn("Invoke listener {} failed, msg: {}.", method.getName(), msg, e);
                }
                return true;
            }

            // topic 同名方法
            if (null != handlerInstance) {
                Method method = Arrays.stream(this.handlerInstance.getClass().getMethods())
                        .filter(m -> m.getName().equals(msg.getTopic()))
                        .filter(m -> m.getReturnType().equals(void.class))
                        .filter(m -> (m.getParameterCount() == 1 && ActorMsg.class.equals(m.getParameters()[0].getType())) || m.getParameterCount() == 0)
                        .max((m1, m2) -> m2.getParameterCount() - m1.getParameterCount())
                        .orElse(null);
                if (method != null) {
                    try {
                        if (method.getParameterCount() == 1) {
                            method.invoke(handlerInstance, msg);
                            return true;
                        } else if (method.getParameterCount() == 0) {
                            method.invoke(handlerInstance);
                            return true;
                        }
                    } catch (Exception e) {
                        logger.warn("Invoke listener {} failed, msg: {}.", method.getName(), msg, e);
                        return true;
                    }
                }
            }

            // 默认方法
            if (null != defaultHandler) {
                try {
                    defaultHandler.accept(msg);
                } catch (Exception e) {
                    logger.warn("Invoke listener {} failed, msg: {}.", defaultHandler, msg, e);

                }
            } else {
                logger.warn("No handler for msg: {}", msg);
            }
            return true;
        }
        return false;
    }

    void receive(ActorMsg msg) {
        msgQueue.add(msg);
        ring();
    }

    private Object ring;
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
