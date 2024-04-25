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

    }


    private void addActor(Actor actor) {
        inboxMap.put(actor.getInbox().getMyAddr(), actor.getInbox());
        actor.getInbox().getSubscribedTopics().forEach(topic -> this.subTopic(topic, actor));
        actor.getInbox().getSchedulers().forEach(t -> scheduleActor.addTask(t.second().timeUnit(), t.second().interval(), actor.getAddr(), t.first()));

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

    public void start() {
        for (Postman postman : postmanList) {
            new Thread(postman, "Postman-" + postmanList.indexOf(postman)).start();
        }
    }

    public void stop() {
        for (Postman postman: postmanList) {
            postman.stop();
        }
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
