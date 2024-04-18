package io.journalkeeper.utils.actor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PostOffice{

    private final Map<String, ActorInbox> inboxMap = new HashMap<>();
    private final List<Postman> postmanList = new ArrayList<>();
    private int postmanIndex = 0;
    private final static int DEFAULT_POSTMAN_COUNT = 1;
    public final static String PUB_ADDR = "PUB";

    private final Map<String, Set<ActorInbox>> pubSubMap = new ConcurrentHashMap<>();
    private final ScheduleActor scheduleActor = new ScheduleActor();

    public PostOffice() {
        this(DEFAULT_POSTMAN_COUNT);
        ActorInbox pubSubInbox = new ActorInbox(PUB_ADDR, null);
        inboxMap.put(pubSubInbox.getMyAddr(), pubSubInbox);
        nextPostman().addInbox(pubSubInbox);
        pubSubInbox.setDefaultHandlerFunction(this::pubMsg);
        nextPostman().addOutbox(scheduleActor.getActor().getOutbox());
    }

    public PostOffice(int postmanCount) {
        for (int i = 0; i < postmanCount; i++) {
            postmanList.add(new Postman(this));
        }
    }

    public void addActor(Actor actor) {
        inboxMap.put(actor.getInbox().getMyAddr(), actor.getInbox());
        actor.getInbox().getSubscribedTopics().forEach(topic -> this.subTopic(topic, actor));
        actor.getInbox().getSchedulers().forEach(s -> scheduleActor.addTask(s.timeUnit(), s.interval(), actor.getAddr(), s.topic()));
        nextPostman().addInbox(actor.getInbox());
        nextPostman().addOutbox(actor.getOutbox());
    }

    public void subTopic(String topic, Actor actor) {
        pubSubMap.computeIfAbsent(topic, k -> new HashSet<>()).add(actor.getInbox());
    }


    private Postman nextPostman() {
        if (postmanIndex >= postmanList.size()) {
            postmanIndex = 0;
        }
        return postmanList.get(postmanIndex++);
    }
    void send(ActorMsg msg) {
        ActorInbox inbox = inboxMap.get(msg.getReceiver());
        if (inbox == null) {
            throw new IllegalArgumentException("Receiver not found!");
        }
        inbox.receive(msg);
    }

    private void pubMsg(ActorMsg msg) {
        assert PUB_ADDR.equals(msg.getReceiver());
        Set<ActorInbox> subscribers = pubSubMap.get(msg.getTopic());
        if (subscribers == null) {
            return;
        }
        for (ActorInbox inbox : subscribers) {
            ActorMsg newMsg = new ActorMsg(msg.getSequentialId(), msg.getSender(), inbox.getMyAddr(), msg.getTopic(), msg.getPayload());
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
}
