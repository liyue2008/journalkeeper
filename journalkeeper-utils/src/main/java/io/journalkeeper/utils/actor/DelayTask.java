package io.journalkeeper.utils.actor;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

class DelayTask {
    private final TimeUnit timeUnit;
    private final long delay;
    private final String addr;
    private final String topic;

    public DelayTask(TimeUnit timeUnit, long delay, String addr, String topic) {
        this.timeUnit = timeUnit;
        this.delay = delay;
        this.addr = addr;
        this.topic = topic;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public long getDelay() {
        return delay;
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
        DelayTask that = (DelayTask) o;
        return delay == that.delay && timeUnit == that.timeUnit && Objects.equals(addr, that.addr) && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeUnit, delay, addr, topic);
    }

    @Override
    public String toString() {
        return "ScheduleTask{" +
                "timeUnit=" + timeUnit +
                ", interval=" + delay +
                ", addr='" + addr + '\'' +
                ", topic='" + topic + '\'' +
                '}';
    }
}
