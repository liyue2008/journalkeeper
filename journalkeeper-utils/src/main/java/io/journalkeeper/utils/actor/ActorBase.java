package io.journalkeeper.utils.actor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public abstract class ActorBase implements Actor, Runnable {

    private static final Logger logger = LoggerFactory.getLogger( ActorBase.class );
    private final AtomicLong msgId = new AtomicLong(0);
    private final Queue<ActorMsg> msgQueue;
    private final PostOffice postOffice;

    private final Map<Long, ActorMsg> ongoingRequests = new HashMap<>();

    private final Map<String /* topic */, Consumer<ActorMsg>> handlers = new HashMap<>();

    private Consumer<ActorMsg> defaultHandler = null;

    private BiConsumer<ActorMsg, ActorResponse> defaultResponseHandler = null;

    private final Map<String /* topic */, BiConsumer<ActorMsg, ActorResponse>> responseHandlers = new HashMap<>();
    public final static int DEFAULT_CAPACITY = 1000;

    protected ActorBase(int capacity, PostOffice postOffice) {
        this.msgQueue = new ArrayBlockingQueue<>(capacity);
        this.postOffice = postOffice;
        registerHandler(Topic.RESPONSE, this::processResponse);
        this.postOffice.regInbox(addr(), this);
    }

    private void processResponse(ActorMsg actorMsg) {
        ActorResponse response = actorMsg.getPayload();
        ActorMsg request = ongoingRequests.remove(response.getSequentialId());
        if (request != null) {
            BiConsumer<ActorMsg, ActorResponse> handler = responseHandlers.get(response.getTopic());
            if (handler != null) {
                handler.accept(request, response);
            } else if (this.defaultResponseHandler != null) {
                this.defaultResponseHandler.accept(request, response);
            } else if (responseFutures.containsKey(request)) {
                //noinspection unchecked
                responseFutures.remove(request).complete(response.getResult());
            } else {
                try {
                    Method method = this.getClass().getMethod(actorMsg.getTopic() + "Response", ActorMsg.class, ActorResponse.class);
                    method.invoke(this, request, response) ;
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                    logger.warn("No response handler for topic: {}", response.getTopic());
                }
            }
        } else {
            logger.warn("No ongoing request for response: {}", response);
        }
    }

    @Override
    public boolean receive(ActorMsg msg) {
        return msgQueue.offer(msg);
    }

    protected void send(String addr, String topic, Object payload){
        this.postOffice.send(new ActorMsg(msgId.getAndIncrement(), addr(), addr, topic, payload));
    }

    @SuppressWarnings("rawtypes")
    private final Map<ActorMsg, CompletableFuture> responseFutures = new ConcurrentHashMap<>();

    protected <T> CompletableFuture<T> sendF(String addr, String topic, Object payload){
        CompletableFuture<T> future = new CompletableFuture<>();
        ActorMsg request = new ActorMsg(msgId.getAndIncrement(), addr(), addr, topic, payload);
        responseFutures.put(request, future);
        this.postOffice.send(request);
        return future;
    }

    protected void reply(ActorMsg request, Object result) {
        send(request.getSender(), Topic.RESPONSE, new ActorResponse(request.getTopic(), request.getSequentialId(), result));
    }
    protected void registerHandler(String topic, Consumer<ActorMsg> handler) {
        handlers.put(topic, handler);
    }

    protected void registerResponseHandler(String topic, BiConsumer<ActorMsg, ActorResponse> handler) {
        responseHandlers.put(topic, handler);
    }

    public Consumer<ActorMsg> getDefaultHandler() {
        return defaultHandler;
    }

    public ActorBase setDefaultHandler(Consumer<ActorMsg> defaultHandler) {
        this.defaultHandler = defaultHandler;
        return this;
    }

    public BiConsumer<ActorMsg, ActorResponse> getDefaultResponseHandler() {
        return defaultResponseHandler;
    }

    public void setDefaultResponseHandler(BiConsumer<ActorMsg, ActorResponse> defaultResponseHandler) {
        this.defaultResponseHandler = defaultResponseHandler;
    }

    @Override
    public void run() {
        processOneMsg();
    }

    void processOneMsg(){
        ActorMsg msg = msgQueue.poll();
        if (msg != null) {
            Consumer<ActorMsg> consumer = handlers.get(msg.getTopic());
            if (consumer != null) {
                consumer.accept(msg);
            } else if (null != defaultHandler) {
                defaultHandler.accept(msg);
            } else {
                    Method method = Arrays.stream(this.getClass().getMethods())
                            .filter(m -> m.getName().equals(msg.getTopic()))
                            .filter(m -> m.getReturnType().equals(void.class))
                            .filter(m -> ( m.getParameterCount() == 1 && ActorMsg.class.equals(m.getParameters()[0].getType())) || m.getParameterCount() == 0)
                            .max((m1, m2) -> m2.getParameterCount() - m1.getParameterCount())
                            .orElse(null);
                    if (method != null) {
                        try {
                            if (method.getParameterCount() == 1) {
                                method.invoke(this, msg);
                            } else if (method.getParameterCount() == 0) {
                                method.invoke(this);
                            }
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            reply(msg, "no handler for topic: " + msg.getTopic() + "!");
                        }
                    }
            }
        }
    }
}
