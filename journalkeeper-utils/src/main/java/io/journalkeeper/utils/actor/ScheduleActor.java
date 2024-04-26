package io.journalkeeper.utils.actor;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;

class ScheduleActor {
    private final Actor actor= Actor.builder("Scheduler").build();
    private Timer timer;

    void addTask(ScheduleTask task) {
        if (null == timer) {
            timer = new Timer("ScheduleActorTimer", true);
        }
        long delay = task.getTimeUnit().toMillis(task.getInterval());
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                actor.send(task.getAddr(), task.getTopic());
            }
        },ThreadLocalRandom.current().nextLong(delay), delay);
    }

    Actor getActor() {
        return actor;
    }
}
