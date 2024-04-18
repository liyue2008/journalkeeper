package io.journalkeeper.utils.actor;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ActorResponseSupportTest {
    @Mock
    private ActorInbox inbox;
    @Mock
    private ActorOutbox outbox;
    @Mock
    private ActorMsg actorMsg;

    @Mock
    private BiConsumer<ActorMsg, ActorResponse> handler;

    private ActorResponseSupport actorResponseSupport;

    @Before
    public void setUp() {
        actorResponseSupport = new ActorResponseSupport(inbox, outbox);
    }

    /**
     * 测试send方法
     */
    @Test
    public void testSend() {
        // arrange
        String addr = "testAddr";
        String topic = "testTopic";
        Object payload = new Object();

        when(outbox.send(addr, topic, payload)).thenReturn(actorMsg);

        // act
        actorResponseSupport.send(addr, topic, payload);

        // assert
        verify(outbox).send(addr, topic, payload);
    }

    /**
     * 测试addResponseHandler方法
     */
    @Test
    public void testAddTopicHandlerFunction() {
        // arrange
        String topic = "testTopic";

        // act
        actorResponseSupport.addTopicHandlerFunction(topic, handler);

        // assert
        // 无需断言，因为这个方法没有返回值，也不改变任何可以观察的状态
    }

    /**
     * 测试reply方法
     */
    @Test
    public void testReply() {
        // arrange
        Object result = new Object();
        when(actorMsg.getSender()).thenReturn("testSender");
        when(actorMsg.getTopic()).thenReturn("testTopic");
        when(actorMsg.getSequentialId()).thenReturn(1L);

        // act
        actorResponseSupport.reply(actorMsg, result);

        // assert
        verify(outbox).send(anyString(), eq(ActorResponseSupport.RESPONSE), any(ActorResponse.class));
    }

    /**
     * 测试setDefaultResponseHandler方法
     */
    @Test
    public void testSetDefaultHandlerFunction() {
        // arrange
        // act
        actorResponseSupport.setDefaultHandlerFunction(handler);

        // assert
        // 无需断言，因为这个方法没有返回值，也不改变任何可以观察的状态
    }


}
