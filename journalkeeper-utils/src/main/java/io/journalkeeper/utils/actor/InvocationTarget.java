package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.ActorScheduler;
import io.journalkeeper.utils.actor.annotation.ActorSubscriber;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

class InvocationTarget {

    enum TargetType {
        SUBSCRIBER,
        LISTENER,
        SCHEDULER
    }
    private final Object target;
    private final Method method;
    private final TargetType type;
    private final String topic;

    // for scheduler only
    private final TimeUnit timeUnit;
    private final long interval;
    public InvocationTarget(Object target, Method method, TargetType type, String topic) {
        this(target, method, type, topic, null, 0);
    }

    public InvocationTarget(Object target, Method method, TargetType type, String topic, TimeUnit timeUnit, long interval) {
        this.target = target;
        this.method = method;
        this.type = type;
        this.timeUnit = timeUnit;
        this.interval = interval;
        this.topic = topic;
    }
    public InvocationTarget(Object target, Method method) {
        this.target = target;
        this.method = method;
        this.topic = ActorUtils.methodToTopic(method);

        if (method.isAnnotationPresent(ActorSubscriber.class)) {
            this.type = TargetType.SUBSCRIBER;
            timeUnit = null;
            interval = 0;
        } else if (method.isAnnotationPresent(ActorScheduler.class)) {
            this.type =  TargetType.SCHEDULER;
            ActorScheduler scheduler = method.getAnnotation(ActorScheduler.class);
            this.timeUnit = scheduler.timeUnit();
            this.interval = scheduler.interval();
        } else {
            this.type =  TargetType.LISTENER;
            timeUnit = null;
            interval = 0;
        }
    }

    public Object getTarget() {
        return target;
    }

    public Method getMethod() {
        return method;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InvocationTarget that = (InvocationTarget) o;
        return Objects.equals(target, that.target) && Objects.equals(method, that.method);
    }

    @Override
    public int hashCode() {
        return Objects.hash(target, method);
    }

    public TargetType getType() {
        return type;
    }


    public String getTopic() {
        return topic;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public long getInterval() {
        return interval;
    }
}
