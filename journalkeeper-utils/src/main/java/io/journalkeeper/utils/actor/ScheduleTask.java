package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.ActorScheduler;

import java.util.concurrent.TimeUnit;

class ScheduleTask {
    private final TimeUnit timeUnit;
    private final long interval;
    private final String addr;
    private final String topic;

    public ScheduleTask(String addr, String topic, ActorScheduler scheduler){
        this.timeUnit = scheduler.timeUnit();
        this.interval = scheduler.interval();
        this.addr = addr;
        this.topic = topic;
    }
    public ScheduleTask(TimeUnit timeUnit, long interval, String addr, String topic) {
        this.timeUnit = timeUnit;
        this.interval = interval;
        this.addr = addr;
        this.topic = topic;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public long getInterval() {
        return interval;
    }

    public String getAddr() {
        return addr;
    }

    public String getTopic() {
        return topic;
    }
}
