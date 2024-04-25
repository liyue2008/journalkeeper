package io.journalkeeper.utils.actor;

public class ActorResponse {
    private final ActorMsg request;
    private final Object result;
    private final Throwable throwable;

    public ActorResponse(ActorMsg request, Object result) {
        this.request = request;
        this.result = result;
        this.throwable = null;
    }

    public ActorResponse(ActorMsg request, Throwable throwable) {
        this.request = request;
        this.result = null;
        this.throwable = throwable;
    }


    public ActorMsg getRequest() {
        return request;
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
                "request=" + request +
                ", result=" + result +
                ", throwable=" + throwable +
                '}';
    }
}
