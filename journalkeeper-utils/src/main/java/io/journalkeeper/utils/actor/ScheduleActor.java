package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.threads.NamedThreadFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

class ScheduleActor {
    private final Actor actor= Actor.builder("Scheduler").setHandlerInstance(this).build();
    private ScheduledExecutorService executorService;
    private final Map<String /* addr-topic */, ScheduledFuture<?>> runningTasks = new HashMap<>();
    private boolean stopped = false;
    private final String name;

    ScheduleActor(String name) {
        this.name = name;
    }

    @ActorListener
    void addTask(ScheduleTask task) {
        if (stopped) {
            throw new IllegalStateException("ScheduleActor has been stopped");
        }
        if (null == executorService) {
            executorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("ActorScheduler-" + (name.isEmpty() ? "" : (name + "-"))));
        }
        long delay = task.getInterval();
        ScheduledFuture<?> scheduledFuture = executorService.scheduleAtFixedRate(() -> actor.send(task.getAddr(), task.getTopic()), ThreadLocalRandom.current().nextLong(delay), delay, TimeUnit.MILLISECONDS);
        runningTasks.put(task.getAddr() + "-" + task.getTopic(), scheduledFuture);
    }

    @ActorListener
    private void removeTask(String addr, String topic) {
        ScheduledFuture<?> scheduledFuture = runningTasks.remove(addr + "-" + topic);
        if (null != scheduledFuture) {
            scheduledFuture.cancel(false);
        }
    }

    Actor getActor() {
        return actor;
    }

    void stop () throws InterruptedException {
        runningTasks.values().forEach(f -> f.cancel(false));
        runningTasks.clear();
        if (null != executorService) {
            executorService.shutdown();
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
            executorService = null;
        }
    }
}
