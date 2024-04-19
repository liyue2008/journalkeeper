package io.journalkeeper.utils.actor;

import java.util.Arrays;

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

    public ActorMsg(long sequentialId, String sender, String receiver, String topic, Object... payloads) {
        this.sequentialId = sequentialId;
        this.sender = sender;
        this.receiver = receiver;
        this.topic = topic;
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

    public <T> T getPayload(int index) {
        //noinspection unchecked
        return (T) payloads[index];
    }

    public String getTopic() {
        return this.topic;
    }

    @Override
    public String toString() {
        return "ActorMsg{" +
                "sequentialId=" + sequentialId +
                ", sender='" + sender + '\'' +
                ", receiver='" + receiver + '\'' +
                ", topic='" + topic + '\'' +
                ", payloads=" + Arrays.toString(payloads) +
                '}';
    }
}
