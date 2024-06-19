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

    private final Response response;

    public enum Response {
        REQUIRED, // 要求返回响应
        DEFAULT, // 默认值，是否返回响应由接收着决定
        IGNORE // 不返回响应
    }
    public ActorMsg(long sequentialId, String sender, String receiver, String topic, Object... payloads) {
        this(sequentialId, sender, receiver, topic, Response.DEFAULT, payloads);
    }

    public ActorMsg(long sequentialId, String sender, String receiver, String topic, Response response, Object... payloads) {
        this.sequentialId = sequentialId;
        this.sender = sender;
        this.receiver = receiver;
        this.topic = topic;
        this.response = response;
        this.payloads = payloads;
    }

    public long getSequentialId() {
        return sequentialId;
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

    public Response getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return "ActorMsg{" +
                "sequentialId=" + sequentialId +
                ", sender='" + sender + '\'' +
                ", receiver='" + receiver + '\'' +
                ", topic='" + topic + '\'' +
                ", payloads=" + Arrays.toString(payloads) +
                ", response=" + response +
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
}
