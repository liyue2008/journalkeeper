package io.journalkeeper.utils.actor;

public class ActorResponse {

    private final String topic;
    private final long sequentialId;

    private final Object result;

    public ActorResponse(String topic, long sequentialId, Object result) {
        this.topic = topic;
        this.sequentialId = sequentialId;
        this.result = result;
    }



    public String getTopic() {
        return topic;
    }

    public long getSequentialId() {
        return sequentialId;
    }

    public <T> T getResult() {
        //noinspection unchecked
        return (T) result;
    }

    @Override
    public String toString() {
        return "ActorResponse{" +
                "topic='" + topic + '\'' +
                ", sequentialId=" + sequentialId +
                ", result=" + result +
                '}';
    }
}
