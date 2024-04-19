package io.journalkeeper.utils.actor;

public class ActorResponse {

    private final String topic;
    private final long sequentialId;

    private final Object result;
    private final Throwable throwable;

    public ActorResponse(String topic, long sequentialId, Object result) {
        this.topic = topic;
        this.sequentialId = sequentialId;
        this.result = result;
        this.throwable = null;
    }

    public ActorResponse(String topic, long sequentialId, Throwable throwable) {
        this.topic = topic;
        this.sequentialId = sequentialId;
        this.result = null;
        this.throwable = throwable;
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

    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public String toString() {
        return "ActorResponse{" +
                "topic='" + topic + '\'' +
                ", sequentialId=" + sequentialId +
                ", result=" + result +
                ", throwable=" + throwable +
                '}';
    }
}
