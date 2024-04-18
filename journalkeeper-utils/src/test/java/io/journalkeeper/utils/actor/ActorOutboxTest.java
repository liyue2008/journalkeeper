package io.journalkeeper.utils.actor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.function.Consumer;

import static org.junit.Assert.*;

public class ActorOutboxTest {
    private ActorOutbox actorOutbox;
    private final String myAddr = "localhost";

    @Before
    public void setUp() {
        actorOutbox = new ActorOutbox(myAddr);
    }

    /**
     * 测试send方法，检查是否能正确发送消息
     */
    @Test
    public void testSend() {
        String addr = "remote";
        String topic = "test";
        String payload = "payload";

        ActorMsg actorMsg = actorOutbox.send(addr, topic, payload);

        assertEquals(myAddr, actorMsg.getSender());
        assertEquals(addr, actorMsg.getReceiver());
        assertEquals(topic, actorMsg.getTopic());
        assertEquals(payload, actorMsg.getPayload());
    }

    /**
     * 测试consumeOneMsg方法，检查是否能正确消费消息
     */
    @Test
    public void testConsumeOneMsg() {
        String addr = "remote";
        String topic = "test";
        String payload = "payload";

        actorOutbox.send(addr, topic, payload);

        Consumer<ActorMsg> consumer = Mockito.mock(Consumer.class);
        boolean result = actorOutbox.consumeOneMsg(consumer);

        assertTrue(result);
        Mockito.verify(consumer, Mockito.times(1)).accept(Mockito.any(ActorMsg.class));
    }

    /**
     * 测试consumeOneMsg方法，当消息队列为空时，应返回false
     */
    @Test
    public void testConsumeOneMsgWhenQueueIsEmpty() {
        Consumer<ActorMsg> consumer = Mockito.mock(Consumer.class);
        boolean result = actorOutbox.consumeOneMsg(consumer);

        assertFalse(result);
        Mockito.verify(consumer, Mockito.never()).accept(Mockito.any(ActorMsg.class));
    }

}
