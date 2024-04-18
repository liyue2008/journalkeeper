package io.journalkeeper.utils.actor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ActorInboxTest {
    private ActorInbox actorInbox;
    private ActorMsg mockMsg;

    @Before
    public void setUp() {
        actorInbox = new ActorInbox(1000, "test", null);
        mockMsg = Mockito.mock(ActorMsg.class);
    }

    /**
     * 测试processOneMsg方法，当消息队列为空时，应返回false
     */
    @Test
    public void testProcessOneMsgWhenQueueIsEmpty() {
        // arrange
        // 不需要设置任何预设条件

        // act
        boolean result = actorInbox.processOneMsg();

        // assert
        assertFalse(result);
    }

    /**
     * 测试processOneMsg方法，当消息队列不为空，且消息主题有对应的处理器时，应正确处理消息并返回true
     */
    @Test
    public void testProcessOneMsgWhenTopicHasHandler() {
        // arrange
        when(mockMsg.getTopic()).thenReturn("testTopic");
        Consumer<ActorMsg> mockHandler = mock(Consumer.class);
        actorInbox.addTopicHandlerFunction("testTopic", mockHandler);
        actorInbox.receive(mockMsg);

        // act
        boolean result = actorInbox.processOneMsg();

        // assert
        assertTrue(result);
        verify(mockHandler).accept(mockMsg);
    }

    /**
     * 测试processOneMsg方法，当消息队列不为空，且消息主题没有对应的处理器，但有对应的注解监听器时，应正确处理消息并返回true
     */
    @Test
    public void testProcessOneMsgWhenTopicHasAnnotationListener() throws Exception {
        // arrange
        when(mockMsg.getTopic()).thenReturn("testTopic");
        when(mockMsg.getPayload()).thenReturn("payload");
        Object handlerInstance = new Object() {
            @ActorListener(topic = "testTopic", payload = true)
            public void testTopic(String payload) {
                assertEquals("payload", payload);
            }
        };
        actorInbox.setHandlerInstance(handlerInstance);
        actorInbox.receive(mockMsg);

        // act
        boolean result = actorInbox.processOneMsg();

        // assert
        assertTrue(result);
        // 由于方法是匿名的，我们无法直接验证它是否被调用，但我们可以通过验证消息的getPayload方法是否被调用来间接验证
        verify(mockMsg).getPayload();
    }

    /**
     * 测试processOneMsg方法，当消息队列不为空，且消息主题没有对应的处理器和注解监听器，但有对应的同名方法时，应正确处理消息并返回true
     */
    @Test
    public void testProcessOneMsgWhenTopicHasSameNameMethod() throws Exception {
        // arrange
        when(mockMsg.getTopic()).thenReturn("testMethod");
        Object handlerInstance = new Object() {
            public void testMethod(ActorMsg msg) {
            }
        };
        actorInbox.setHandlerInstance(handlerInstance);
        actorInbox.receive(mockMsg);

        // act
        boolean result = actorInbox.processOneMsg();

        // assert
        assertTrue(result);
        // 由于方法是匿名的，我们无法直接验证它是否被调用，但我们可以通过验证消息的getTopic方法是否被调用来间接验证
        verify(mockMsg, atLeastOnce()).getTopic();
    }

    /**
     * 测试processOneMsg方法，当消息队列不为空，且消息主题没有对应的处理器、注解监听器和同名方法，但有默认处理器时，应正确处理消息并返回true
     */
    @Test
    public void testProcessOneMsgWhenHasDefaultHandler() {
        // arrange
        when(mockMsg.getTopic()).thenReturn("testTopic");
        Consumer<ActorMsg> mockHandler = mock(Consumer.class);
        actorInbox.setDefaultHandlerFunction(mockHandler);
        actorInbox.receive(mockMsg);

        // act
        boolean result = actorInbox.processOneMsg();

        // assert
        assertTrue(result);
        verify(mockHandler).accept(mockMsg);
    }

    /**
     * 测试processOneMsg方法，当消息队列不为空，且消息主题没有对应的处理器、注解监听器、同名方法和默认处理器时，应返回true
     */
    @Test
    public void testProcessOneMsgWhenNoHandler() {
        // arrange
        when(mockMsg.getTopic()).thenReturn("testTopic");
        actorInbox.receive(mockMsg);

        // act
        boolean result = actorInbox.processOneMsg();

        // assert
        assertTrue(result);
    }
}
