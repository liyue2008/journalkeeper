package io.journalkeeper.utils.actor;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class PostOfficeTest {





    /**
     * 测试send方法，检查是否正确发送消息
     */
    @Test
    public void testSend() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        PostOffice postOffice = new PostOffice();
        Actor actor = Mockito.mock(Actor.class);
        ActorOutbox actorOutbox = new ActorOutbox("receiver");
        ActorInbox actorInbox = new ActorInbox("receiver", actorOutbox);

        when(actor.getInbox()).thenReturn(actorInbox);
        when(actor.getOutbox()).thenReturn(actorOutbox);
        ActorMsg actorMsg = new ActorMsg(1L, "sender", "receiver", "testTopic", "payload");
        CountDownLatch latch = new CountDownLatch(1);
        actorInbox.addTopicHandlerFunction("testTopic", msg -> {
            assertEquals(actorMsg, msg);
            counter.incrementAndGet();
            latch.countDown();
        });

        postOffice.addActor(actor);
        postOffice.start();
        try {
        postOffice.send(actorMsg);
        latch.await();
        assertEquals(1, counter.get());

        } finally {
            postOffice.stop();
        }
    }

    @Test
    public void testPubSub() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        PostOffice postOffice = new PostOffice();
        Actor actor = Mockito.mock(Actor.class);
        ActorOutbox actorOutbox = new ActorOutbox("receiver");
        ActorInbox actorInbox = new ActorInbox("receiver", actorOutbox);

        when(actor.getInbox()).thenReturn(actorInbox);
        when(actor.getOutbox()).thenReturn(actorOutbox);
        ActorMsg actorMsg = new ActorMsg(1L, "sender", PostOffice.PUB_ADDR, "testTopic", "payload");
        CountDownLatch latch = new CountDownLatch(1);
        actorInbox.addTopicHandlerFunction("testTopic", msg -> {
            counter.incrementAndGet();
            latch.countDown();
        });

        postOffice.subTopic("testTopic", actor);
        postOffice.addActor(actor);
        postOffice.start();
        try {
            postOffice.send(actorMsg);
            latch.await();
            assertEquals(1, counter.get());

        } finally {
            postOffice.stop();
        }
    }


}
