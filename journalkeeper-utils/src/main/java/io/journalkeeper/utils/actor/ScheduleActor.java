package io.journalkeeper.utils.actor;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

class ScheduleActor {
    private final Actor actor= new Actor("Scheduler");
    private Timer timer;

    void addTask(TimeUnit timeUnit, long interval, String addr, String topic) {
        if (null == timer) {
            timer = new Timer("ScheduleActorTimer", true);
        }
        long delay = timeUnit.toMillis(interval);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                actor.send(addr, topic, null);
            }
        },ThreadLocalRandom.current().nextLong(delay), delay);
    }

    Actor getActor() {
        return actor;
    }
}
