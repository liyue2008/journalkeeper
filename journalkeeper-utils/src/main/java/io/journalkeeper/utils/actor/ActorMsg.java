package io.journalkeeper.utils.actor;

import java.util.Arrays;
import java.util.Objects;

/**
 * Actor之间传递的消息消息
 */
public class ActorMsg {
    // 消息ID，只在发送者范围内唯一。
    private final long sequentialId;

    private final String sender;

    private final String receiver;

    private final String topic;

    private final Object[] payloads;

    private final ActorMsgCtx context;

    public enum Response {
        REQUIRED, // 要求返回响应
        DEFAULT, // 默认值，是否返回响应由接收着决定
        IGNORE // 不返回响应
    }

    public enum Type {
        REQUEST,
        RESPONSE
    }

    static final String RESPONSE = "actor_response";

    /**
     * create response message
     */
    public ActorMsg(long sequentialId, String sender,ActorMsg request, Object result, Throwable throwable) {
        this(sequentialId, sender, request.getSender(), RESPONSE, new ActorMsgCtx(Response.IGNORE, Type.RESPONSE, ActorRejectPolicy.EXCEPTION, request.getContext().getMetric() != null), request, result, throwable);
    }
    public ActorMsg(long sequentialId, String sender, String receiver, String topic, Object... payloads) {
        this(sequentialId, sender, receiver, topic, new ActorMsgCtx(), payloads);
    }

    public ActorMsg(long sequentialId, String sender, String receiver, String topic, ActorMsgCtx context, Object... payloads) {
        this.sequentialId = sequentialId;
        this.sender = sender;
        this.receiver = receiver;
        this.topic = topic;
        this.context = context;
        this.payloads = payloads;
    }

    public String getSender() {
        return sender;
    }

    public String getReceiver() {
        return receiver;
    }

    public <T> T getPayload() {
        return getPayload(0);
    }

    public Object[] getPayloads() {
        return payloads;
    }

    @SuppressWarnings("unchecked")
    public <T> T getPayload(int index) {
        return (T) payloads[index];
    }

    public String getTopic() {
        return this.topic;
    }

    public String getQueueName() {
        if (context.getType() == Type.REQUEST) {
            return this.topic;
        } else {
            return this.getRequest().getQueueName();
        }
    }

    public ActorMsgCtx getContext() {
        return context;
    }

    @Override
    public String toString() {
        return "{" +
                sequentialId +
                " | " + topic + " | " +
                sender + " --> " + receiver + " | " +
                Arrays.toString(payloads) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActorMsg actorMsg = (ActorMsg) o;
        return sequentialId == actorMsg.sequentialId && Objects.equals(sender, actorMsg.sender);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequentialId, sender);
    }

    public ActorMsg getRequest() {
        if (context.getType() != Type.RESPONSE) {
            throw new IllegalStateException("Not a response message!");
        }
        return getPayload(0);
    }

    public <T> T getResult() {
        if (context.getType() != Type.RESPONSE) {
            throw new IllegalStateException("Not a response message!");
        }
        return getPayload(1);
    }

    public Throwable getThrowable() {
        if (context.getType() != Type.RESPONSE) {
            throw new IllegalStateException("Not a response message!");
        }
        return getPayload(2);
    }
}
