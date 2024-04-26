package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.ActorMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PostOffice{

    private final Map<String, ActorInbox> inboxMap = new HashMap<>();
    private final List<Postman> postmanList;
    private final static int DEFAULT_POSTMAN_COUNT = 1;
    public final static String PUB_ADDR = "PUB";

    private final Map<String, Set<ActorInbox>> pubSubMap = new ConcurrentHashMap<>();
    private final ScheduleActor scheduleActor = new ScheduleActor();

    private PostOffice(int threadCount, List<Actor> actorList) {
        Actor pubSubActor = Actor.builder(PUB_ADDR).setDefaultHandlerFunction(this::pubMsg).build();
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


    // TODO: 需要支持动态添加定时任务
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
        assert PUB_ADDR.equals(msg.getReceiver());
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
            Thread thread = new Thread(postman, "Postman-" + postmanList.indexOf(postman));
            thread.setDaemon(true);
            thread.start();
        }
    }

    private void stop() {
        // TODO：清理完未处理的消息

    }

    public static Builder builder() {
        return new Builder();
    }
    public static class Builder {
        private Builder() {}
        private int threadCount = DEFAULT_POSTMAN_COUNT;
        private final List<Actor> actorList = new ArrayList<>();

        public Builder threadCount(int threadCount) {
            this.threadCount = threadCount;
            return this;
        }

        public Builder addActor(Actor actor) {
            this.actorList.add(actor);
            return this;
        }

        public PostOffice build() {
            return new PostOffice(threadCount, actorList);
        }
    }
}
