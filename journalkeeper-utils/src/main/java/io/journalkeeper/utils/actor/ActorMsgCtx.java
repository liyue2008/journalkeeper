package io.journalkeeper.utils.actor;

import java.util.Objects;

public class ActorMsgCtx {
    private final ActorMsg.Response responseConfig;
    private final ActorMsg.Type type;
    private final ActorRejectPolicy rejectPolicy;
    private final ActorMetric metric;
    public ActorMsgCtx() {
        this(ActorMsg.Response.DEFAULT, ActorMsg.Type.REQUEST, ActorRejectPolicy.EXCEPTION);
    }
    public ActorMsgCtx(ActorMsg.Response responseConfig, ActorMsg.Type type, ActorRejectPolicy rejectPolicy, boolean enableMetric) {
        this.responseConfig = responseConfig;
        this.type = type;
        this.rejectPolicy = rejectPolicy;
        this.metric = enableMetric ? new ActorMetric() : null;
    }
    public ActorMsgCtx(ActorMsg.Response responseConfig, ActorMsg.Type type, ActorRejectPolicy rejectPolicy) {
        this(responseConfig, type, rejectPolicy, false);
    }

    public ActorMsg.Response getResponseConfig() {
        return responseConfig;
    }

    public ActorMsg.Type getType() {
        return type;
    }

    public ActorRejectPolicy getRejectPolicy() {
        return rejectPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActorMsgCtx that = (ActorMsgCtx) o;
        return responseConfig == that.responseConfig && type == that.type && rejectPolicy == that.rejectPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(responseConfig, type, rejectPolicy);
    }

    @Override
    public String toString() {
        return "ActorMsgCtx{" +
                "responseConfig=" + responseConfig +
                ", type=" + type +
                ", rejectPolicy=" + rejectPolicy +
                '}';
    }

    public ActorMetric getMetric() {
        return metric;
    }
}
