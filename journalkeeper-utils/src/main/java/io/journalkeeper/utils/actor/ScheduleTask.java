package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.ActorScheduler;

import java.util.Objects;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScheduleTask that = (ScheduleTask) o;
        return interval == that.interval && timeUnit == that.timeUnit && Objects.equals(addr, that.addr) && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeUnit, interval, addr, topic);
    }

    @Override
    public String toString() {
        return "ScheduleTask{" +
                "timeUnit=" + timeUnit +
                ", interval=" + interval +
                ", addr='" + addr + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}
