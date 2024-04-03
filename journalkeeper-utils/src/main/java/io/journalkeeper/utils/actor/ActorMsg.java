package io.journalkeeper.utils.actor;

/**
 * Actor之间传递的消息消息
 */
public class ActorMsg {
    // 消息ID，只在发送者范围内唯一。
    private final long sequentialId;

    private final String sender;

    private final String receiver;

    private final String topic;
    private final Object payload;

    public ActorMsg(long sequentialId, String sender, String receiver, String topic, Object payload) {
        this.sequentialId = sequentialId;
        this.sender = sender;
        this.receiver = receiver;
        this.topic = topic;
        this.payload = payload;
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
        //noinspection unchecked
        return (T) payload;
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
                ", payload=" + payload +
                '}';
    }
}
