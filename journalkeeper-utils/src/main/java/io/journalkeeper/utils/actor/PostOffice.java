package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.ActorMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PostOffice{

    private static final Logger logger = LoggerFactory.getLogger(PostOffice.class);
    private final Map<String, ActorInbox> inboxMap = new HashMap<>();
    private final List<Postman> postmanList;
    private final static int DEFAULT_POSTMAN_COUNT = 1;
    public final static String PUB_ADDR = "PUB";
    private final Actor pubSubActor;
    private final Map<String, Set<ActorInbox>> pubSubMap = new ConcurrentHashMap<>();
    private final ScheduleActor scheduleActor;
    private final List<Actor> actorList;
    private final String name;

    private PostOffice(int threadCount, List<Actor> actorList, String name) {
        this.name = null == name ? "" : name;
        this.scheduleActor = new ScheduleActor(this.name);
        this.actorList = Collections.unmodifiableList(actorList);
        pubSubActor = Actor.builder(PUB_ADDR).setDefaultHandlerFunction(this::pubMsg).build();
        List<Actor> allActors = new ArrayList<>(actorList.size() + 2);
        allActors.add(pubSubActor);
        allActors.add(scheduleActor.getActor());
        allActors.addAll(actorList);
        allActors.forEach(this::addActor);

        List<List<ActorInbox>> threadInboxList = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            threadInboxList.add(new ArrayList<>());
        }
        List<List<ActorOutbox>> threadOutboxList = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            threadOutboxList.add(new ArrayList<>());
        }
        int threadIndex = 0;
        for (Actor actor: allActors) {
            threadInboxList.get(threadIndex++ % threadCount).add(actor.getInbox());
            threadOutboxList.get(threadIndex++ % threadCount).add(actor.getOutbox());
        }

        this.postmanList = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Postman.Builder builder = Postman.builder().postOffice(this);
            threadInboxList.get(i).forEach(builder::addInbox);
            threadOutboxList.get(i).forEach(builder::addOutbox);
            this.postmanList.add(builder.build());
        }
        start();
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }


    private void addActor(Actor actor) {
        inboxMap.put(actor.getInbox().getMyAddr(), actor.getInbox());
        actor.getInbox().getSubscribedTopics().forEach(topic -> this.subTopic(topic, actor));
        actor.getInbox().getSchedulers().forEach(scheduleActor::addTask);
    }

    private void subTopic(String topic, Actor actor) {
        pubSubMap.computeIfAbsent(topic, k -> new HashSet<>()).add(actor.getInbox());
    }


    void send(ActorMsg msg) {
        ActorInbox inbox = inboxMap.get(msg.getReceiver());
        if (inbox == null) {
            throw new IllegalArgumentException("Receiver not found!");
        }
        inbox.receive(msg);
    }

    private void pubMsg(@ActorMessage ActorMsg msg) {
        Set<ActorInbox> subscribers = pubSubMap.get(msg.getTopic());
        if (subscribers == null) {
            return;
        }
        for (ActorInbox inbox : subscribers) {
            ActorMsg newMsg = new ActorMsg(msg.getSequentialId(), msg.getSender(), inbox.getMyAddr(), msg.getTopic(), msg.getPayloads());
            inbox.receive(newMsg);
        }
    }

    private void start() {
        for (Postman postman : postmanList) {
            Thread thread = new Thread(postman, "Postman-" + (name.isEmpty() ? "" : (name + "-")) + postmanList.indexOf(postman));
            thread.setDaemon(true);
            thread.start();
        }
    }

    public void stop() {
        try {
            // 停止接收新的定时任务
            // 取消所有定时任务
            scheduleActor.getActor().sendThen("Scheduler","stop");

            // 停止并等待pubsubActor
            this.pubSubActor.stop();
            while (!(pubSubActor.outboxCleared() && pubSubActor.inboxCleared())) {
                //noinspection BusyWait
                Thread.sleep(10);
            }
            // 停止接收新的消息
            actorList.forEach(Actor::stop);
            // 处理所有发件箱的消息
            while (actorList.stream().anyMatch(actor -> !actor.outboxCleared())) {
                //noinspection BusyWait
                Thread.sleep(10);
            }
            // 处理所有收件箱的消息
            while (actorList.stream().anyMatch(actor -> !actor.inboxCleared())) {
                //noinspection BusyWait
                Thread.sleep(10);
            }
            // 停止所有Postman线程
            for (Postman postman : postmanList) {
                postman.stop();
            }
        } catch (InterruptedException e) {
            logger.warn("Stop post office exception!", e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
    public static class Builder {
        private Builder() {}
        private int threadCount = DEFAULT_POSTMAN_COUNT;
        private final List<Actor> actorList = new ArrayList<>();
        private String name = null;

        public Builder threadCount(int threadCount) {
            this.threadCount = threadCount;
            return this;
        }

        public Builder addActor(Actor actor) {
            this.actorList.add(actor);
            return this;
        }

        public PostOffice build() {
            return new PostOffice(threadCount, actorList, name);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }
    }
}
