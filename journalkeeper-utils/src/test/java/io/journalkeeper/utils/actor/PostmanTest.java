package io.journalkeeper.utils.actor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PostmanTest {
    private PostOffice postOffice;
    private Postman postman;

    @Before
    public void setUp() {
        postOffice = Mockito.mock(PostOffice.class);
        postman = new Postman(postOffice);
    }

    /**
     * 测试添加 ActorInbox
     */
    @Test
    public void testAddInbox() {
        ActorInbox inbox = Mockito.mock(ActorInbox.class);
        postman.addInbox(inbox);
        verify(inbox, times(1)).setRing(any());
    }

    /**
     * 测试添加 ActorOutbox
     */
    @Test
    public void testAddOutbox() {
        ActorOutbox outbox = Mockito.mock(ActorOutbox.class);
        postman.addOutbox(outbox);
        verify(outbox, times(1)).setRing(any());
    }

    /**
     * 测试线程已启动时添加 ActorInbox 抛出异常
     */
    @Test(expected = IllegalStateException.class)
    public void testAddInboxWhenThreadStarted() {
        Thread thread = new Thread(postman);
        thread.start();
        ActorInbox inbox = Mockito.mock(ActorInbox.class);
        postman.addInbox(inbox);
    }

    /**
     * 测试线程已启动时添加 ActorOutbox 抛出异常
     */
    @Test(expected = IllegalStateException.class)
    public void testAddOutboxWhenThreadStarted() {
        Thread thread = new Thread(postman);
        thread.start();
        ActorOutbox outbox = Mockito.mock(ActorOutbox.class);
        postman.addOutbox(outbox);
    }

    /**
     * 测试停止方法
     */
    @Test
    public void testStop() throws InterruptedException {
        Thread thread = new Thread(postman);
        thread.start();
        postman.stop();
        thread.join();
        assertFalse(thread.isAlive());
    }
}
