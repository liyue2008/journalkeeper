package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@SuppressWarnings("ALL")
public class ActorTest {

    @Test
    public void testAddTopicHandlerFunctionNoArg() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        Actor receiver = Actor.builder().addr("receiver")
                .addTopicHandlerFunction("test", latch::countDown)
                .build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
            sender.send("receiver", "test");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testAddTopicHandlerFunctionOneArg() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Actor receiver = Actor.builder().addr("receiver")
                .addTopicHandlerFunction("test", (Consumer<String>) str -> {
                    Assert.assertEquals("Hello, world!", str);
                    latch.countDown();
                })
                .build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();

            sender.send("receiver", "test", "Hello, world!");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testAddTopicHandlerFunctionTwoArgs() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Actor receiver = Actor.builder().addr("receiver")
                .addTopicHandlerFunction("test", (str, num) -> {
                    Assert.assertEquals("Hello, world!", str);
                    Assert.assertEquals(12, num);
                    latch.countDown();
                }).build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        
            sender.send("receiver", "test", "Hello, world!", 12);
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    @Test
    public void testAddTopicHandlerFunctionNoArgWithResponse() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addTopicHandlerFunction("test", () -> {
            latch.countDown();
            return "Hello, world!";
        }).build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        
            sender.sendThen("receiver", "test").thenAccept(str -> {
                Assert.assertEquals("Hello, world!", str);
                latch.countDown();
            });
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    @Test
    public void testAddTopicHandlerFunctionOneArgWithResponse() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addTopicHandlerFunction("test", str -> {
            Assert.assertEquals("Hello, world!", str);
            latch.countDown();
            return "Hello, world2!";
        }).build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.sendThen("receiver", "test", "Hello, world!").thenAccept(str -> {
                Assert.assertEquals("Hello, world2!", str);
                latch.countDown();
            });
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    @Test
    public void testAddTopicHandlerFunctionTwoArgsWithResponse() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addTopicHandlerFunction("test", (str, num) -> {
            Assert.assertEquals("Hello, world!", str);
            Assert.assertEquals(12, num);
            latch.countDown();
            return "Hello, world2!";
        }).build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.sendThen("receiver", "test", "Hello, world!", 12).thenAccept(str -> {
                Assert.assertEquals("Hello, world2!", str);
                latch.countDown();
            });
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    // TOPIC 同名方法

    @SuppressWarnings("SameReturnValue")
    private static class TopicNameHandlerFunctions {
        private final CountDownLatch latch;

        public TopicNameHandlerFunctions(CountDownLatch latch) {
            this.latch = latch;
        }

        private void noArg() {
            latch.countDown();
        }

        private void oneArg(String str) {
            Assert.assertEquals("oneArg", str);
            latch.countDown();
        }

        private void twoArgs(String str, int num) {
            Assert.assertEquals("twoArgs", str);
            Assert.assertEquals(12, num);
            latch.countDown();
        }

        private void threeArgs(String str, int num, float fnum) {
            Assert.assertEquals("threeArgs", str);
            Assert.assertEquals(12, num);
            Assert.assertEquals(1.2f, fnum, 0.00001);
            latch.countDown();
        }

        private String noArgWithReturn() {
            latch.countDown();
            return "noArgWithReturn";
        }

        private String oneArgWithReturn(String str) {
            Assert.assertEquals("oneArg", str);
            latch.countDown();
            return "oneArgWithReturn";
        }

        private String twoArgsWithReturn(String str, int num) {
            Assert.assertEquals("twoArgs", str);
            Assert.assertEquals(12, num);
            latch.countDown();
            return "twoArgsWithReturn";
        }

        private String threeArgsWithReturn(String str, int num, float fnum) {
            Assert.assertEquals("threeArgs", str);
            Assert.assertEquals(12, num);
            Assert.assertEquals(1.2f, fnum, 0.00001);
            latch.countDown();
            return "threeArgsWithReturn";
        }

    }

    @Test
    public void testAddTopicHandlerFunctionWithTopicName() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(12);
        TopicNameHandlerFunctions handlerFunctions = new TopicNameHandlerFunctions(latch);
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(handlerFunctions).build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "noArg");
            sender.sendThen("receiver", "noArgWithReturn").thenAccept(str -> {
                Assert.assertEquals("noArgWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "oneArg", "oneArg");
            sender.sendThen("receiver", "oneArgWithReturn", "oneArg").thenAccept(str -> {
                Assert.assertEquals("oneArgWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "twoArgs", "twoArgs", 12);
            sender.sendThen("receiver", "twoArgsWithReturn", "twoArgs", 12).thenAccept(str -> {
                Assert.assertEquals("twoArgsWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "threeArgs", "threeArgs", 12, 1.2f);
            sender.sendThen("receiver", "threeArgsWithReturn", "threeArgs", 12, 1.2f).thenAccept(str -> {
                Assert.assertEquals("threeArgsWithReturn", str);
                latch.countDown();
            });
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));


    }

    // annotation

    @SuppressWarnings("SameReturnValue")
    private static class AnnotationHandlerFunctions {
        private final CountDownLatch latch;

        public AnnotationHandlerFunctions(CountDownLatch latch) {
            this.latch = latch;
        }

        @ActorListener
        private void noArg() {
            latch.countDown();
        }

        @ActorListener
        private void oneArg(String str) {
            Assert.assertEquals("oneArg", str);
            latch.countDown();
        }

        @ActorListener
        private void twoArgs(String str, int num) {
            Assert.assertEquals("twoArgs", str);
            Assert.assertEquals(12, num);
            latch.countDown();
        }

        @ActorListener
        private void threeArgs(String str, int num, float fnum) {
            Assert.assertEquals("threeArgs", str);
            Assert.assertEquals(12, num);
            Assert.assertEquals(1.2f, fnum, 0.00001);
            latch.countDown();
        }

        @ActorListener
        private String noArgWithReturn() {
            latch.countDown();
            return "noArgWithReturn";
        }

        @ActorListener
        private String oneArgWithReturn(String str) {
            Assert.assertEquals("oneArg", str);
            latch.countDown();
            return "oneArgWithReturn";
        }

        @ActorListener
        private String twoArgsWithReturn(String str, int num) {
            Assert.assertEquals("twoArgs", str);
            Assert.assertEquals(12, num);
            latch.countDown();
            return "twoArgsWithReturn";
        }

        @ActorListener
        private String threeArgsWithReturn(String str, int num, float fnum) {
            Assert.assertEquals("threeArgs", str);
            Assert.assertEquals(12, num);
            Assert.assertEquals(1.2f, fnum, 0.00001);
            latch.countDown();
            return "threeArgsWithReturn";
        }
    }

    // @ActorMessage

    private static class ActorMsgClass {
        private final CountDownLatch latch;

        public ActorMsgClass(CountDownLatch latch) {
            this.latch = latch;
        }

        private void onMessage(@ActorMessage ActorMsg msg) {
            Assert.assertEquals("sender", msg.getSender());
            Assert.assertEquals("receiver", msg.getReceiver());
            Assert.assertEquals("onMessage", msg.getTopic());
            Assert.assertEquals("Hello", msg.getPayload());
            latch.countDown();
        }

        @ActorListener
        private void onMessageWithAnnotation(@ActorMessage ActorMsg msg) {
            Assert.assertEquals("sender", msg.getSender());
            Assert.assertEquals("receiver", msg.getReceiver());
            Assert.assertEquals("onMessageWithAnnotation", msg.getTopic());
            Assert.assertEquals("Hello", msg.getPayload());
            latch.countDown();
        }
    }

    @Test
    public void testActorMessageAnnotation() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        ActorMsgClass handlerFunctions = new ActorMsgClass(latch);
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(handlerFunctions).build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "onMessage", "Hello");
            sender.send("receiver", "onMessageWithAnnotation", "Hello");

            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    // pub/sub

    private static class Sub {
        private final CountDownLatch latch;

        public Sub(CountDownLatch latch) {
            this.latch = latch;
        }

        @ActorSubscriber
        private void onEvent(String event) {
            Assert.assertEquals("Hello subscriber!", event);
            latch.countDown();
        }
    }

    @Test
    public void testPubSub() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Sub sub = new Sub(latch);
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(sub).build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.pub("onEvent", "Hello subscriber!");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }


    @Test
    public void testPubSubThen() throws InterruptedException, ExecutionException {
        CountDownLatch latch = new CountDownLatch(2);
        Sub sub1 = new Sub(latch);
        Actor receiver1 = Actor.builder().addr("receiver1").setHandlerInstance(sub1).build();
        Sub sub2 = new Sub(latch);
        Actor receiver2 = Actor.builder().addr("receiver2").setHandlerInstance(sub2).build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver1)
                .addActor(receiver2)
                .build();


        sender.pubThen("onEvent", "Hello subscriber!").get();
        Assert.assertEquals(0, latch.getCount());
    }


    // @ActorScheduler

    private static class SchedulerClass {
        private int count = 0;
        @ActorScheduler(interval = 10L)
        private void onEvent() {
            count++;
        }

        private int getCount() {
            return count;
        }
    }

    @Test
    public void testScheduler () throws InterruptedException {
        SchedulerClass schedulerClass = new SchedulerClass();
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(schedulerClass).build();
        PostOffice.builder()
                .addActor(receiver)
                .build();
        
            Thread.sleep(100);
            int count = schedulerClass.getCount();
            Assert.assertTrue(8 < count && count < 12);

    }
    @Test
    public void testRunDelay () throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        Actor actor = Actor.builder().addr("actor").build();
        PostOffice.builder()
                .addActor(actor)
                .build();
        actor.runDelay(50, TimeUnit.MILLISECONDS, counter::incrementAndGet);
        Assert.assertEquals(0, counter.get());
        Thread.sleep(100);
        Assert.assertEquals(1, counter.get());
    }

    @Test
    public void testAddScheduler () throws InterruptedException {
        Actor receiver = Actor.builder().addr("receiver").build();
        AtomicInteger counter = new AtomicInteger();
        PostOffice.builder()
                .addActor(receiver)
                .build();
        receiver.addScheduler(10, TimeUnit.MILLISECONDS, "onEvent", counter::incrementAndGet);
        Thread.sleep(100);
        int count = counter.get();
        Assert.assertTrue(8 < count && count < 12);
    }

    @Test
    public void testRemoveScheduler () throws InterruptedException {
        Actor receiver = Actor.builder().addr("receiver").build();
        AtomicInteger counter = new AtomicInteger();
        PostOffice.builder()
                .addActor(receiver)
                .build();
        receiver.addScheduler(10, TimeUnit.MILLISECONDS, "onEvent", counter::incrementAndGet);
        Thread.sleep(100);
        int count = counter.get();
        Assert.assertTrue(8 < count && count < 12);

        receiver.removeScheduler("onEvent");
        // 等待消息被处理
        Thread.sleep(20);
        // 之后counter不再增长
        count = counter.get();
        Thread.sleep(100);
        Assert.assertEquals(count, counter.get());
    }



    @Test
    public void testAddTopicHandlerFunctionWithAnnotation() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(12);
        AnnotationHandlerFunctions handlerFunctions = new AnnotationHandlerFunctions(latch);
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(handlerFunctions).build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "noArg");
            sender.sendThen("receiver", "noArgWithReturn").thenAccept(str -> {
                Assert.assertEquals("noArgWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "oneArg", "oneArg");
            sender.sendThen("receiver", "oneArgWithReturn", "oneArg").thenAccept(str -> {
                Assert.assertEquals("oneArgWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "twoArgs", "twoArgs", 12);
            sender.sendThen("receiver", "twoArgsWithReturn", "twoArgs", 12).thenAccept(str -> {
                Assert.assertEquals("twoArgsWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "threeArgs", "threeArgs", 12, 1.2f);
            sender.sendThen("receiver", "threeArgsWithReturn", "threeArgs", 12, 1.2f).thenAccept(str -> {
                Assert.assertEquals("threeArgsWithReturn", str);
                latch.countDown();
            });
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));


    }


    @Test
    public void testSetDefaultHandlerFunction() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Actor receiver = Actor.builder().addr("receiver").setDefaultHandlerFunction(msg -> {
            Assert.assertEquals("sender", msg.getSender());
            Assert.assertEquals("receiver", msg.getReceiver());
            Assert.assertEquals("test", msg.getTopic());
            Assert.assertEquals(2, msg.getPayloads().length);
            Assert.assertEquals("Hello, world!", msg.getPayloads()[0]);
            Assert.assertEquals(12, msg.getPayloads()[1]);
            latch.countDown();
        }).build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "test", "Hello, world!", 12);
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    @Test
    public void testSetDefaultHandlerFunctionWithTopicHandler() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger topic1Count = new AtomicInteger(0);
        AtomicInteger topic2Count = new AtomicInteger(0);
        Actor receiver = Actor.builder().addr("receiver").addTopicHandlerFunction("topic1", () -> {
            topic1Count.incrementAndGet();
            latch.countDown();
        }).setDefaultHandlerFunction(msg -> {
            Assert.assertEquals("sender", msg.getSender());
            Assert.assertEquals("receiver", msg.getReceiver());
            Assert.assertEquals("topic2", msg.getTopic());
            topic2Count.incrementAndGet();
            latch.countDown();
        }).build();
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "topic1");
            sender.send("receiver", "topic2");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
            Assert.assertEquals(1, topic1Count.get());
            Assert.assertEquals(1, topic2Count.get());

    }

    @Test
    public void testResponseHandlerFunction() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addTopicHandlerFunction("topic", request -> {
            Assert.assertEquals("Hello!", request);
            latch.countDown();
            return "World!";
        }).build();
        Actor sender = Actor.builder().addr("sender").addResponseHandlerFunction("topic", resp -> {

            Assert.assertEquals("World!", resp.getResult());
            latch.countDown();
        }).build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "topic", "Hello!");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));



    }

    // 注解
    private static class ResponseAnnotationHandlerClass {
        private final CountDownLatch latch;

        public ResponseAnnotationHandlerClass(CountDownLatch latch) {
            this.latch = latch;
        }

        @ActorResponseListener(topic = "my_topic1")
        private void onResponse(ActorMsg response) {
            Assert.assertEquals("result", response.getResult());
            Assert.assertNull(response.getThrowable());
            latch.countDown();
        }
        @ActorResponseListener(topic = "my_topic2")
        private void onException(ActorMsg response) {
            Assert.assertEquals("exception msg", response.getThrowable().getMessage());
            Assert.assertNull(response.getResult());
            latch.countDown();
        }
    }

    @Test
    public void testResponseAnnotationFunction() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(4);
        Actor receiver = Actor.builder().addr("receiver")
                .addTopicHandlerFunction("my_topic1", () -> {
                    latch.countDown();
                    return "result";
                })
                .addTopicHandlerFunction("my_topic2", () -> {
                    latch.countDown();
                    throw new RuntimeException("exception msg");
                })
                .build();
        ResponseAnnotationHandlerClass cls = new ResponseAnnotationHandlerClass(latch);
        Actor sender = Actor.builder().addr("sender").setResponseHandlerInstance(cls).build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "my_topic1");
            sender.send("receiver", "my_topic2");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));


    }


    // 同名方法

    private static class ResponseHandlerClass {
        private final CountDownLatch latch;

        public ResponseHandlerClass(CountDownLatch latch) {
            this.latch = latch;
        }


        private void topicResponse(ActorMsg response) {
            Assert.assertEquals("result", response.getResult());
            Assert.assertNull(response.getThrowable());
            latch.countDown();
        }

    }

    @Test
    public void testResponseFunction() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver")
                .addTopicHandlerFunction("topic", () -> {
                    latch.countDown();
                    return "result";
                })
                .build();
        ResponseHandlerClass cls = new ResponseHandlerClass(latch);
        Actor sender = Actor.builder().addr("sender").setResponseHandlerInstance(cls).build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "topic");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));


    }

    // 默认
    @Test
    public void testDefaultResponseFunction() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(4);
        Actor receiver = Actor.builder().addr("receiver")
                .addTopicHandlerFunction("topic1", () -> {
                    latch.countDown();
                    return "result";
                })
                .addTopicHandlerFunction("topic2", () -> {
                    latch.countDown();
                    return "result";
                })
                .build();
        Actor sender = Actor.builder().addr("sender")
                .setDefaultResponseHandlerFunction(response -> {
                    Assert.assertEquals("result", response.getResult());
                    Assert.assertNull(response.getThrowable());
                    latch.countDown();
                })
                .build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "topic1");
            sender.send("receiver", "topic2");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));


    }

    // reply

    private static class ReplyCls {
        private final CountDownLatch latch;
        private Actor actor;
        private ReplyCls(CountDownLatch latch) {
            this.latch = latch;
        }

        @ResponseManually
        private void topic1(@ActorMessage ActorMsg msg) {
            Assert.assertEquals("Hello", msg.getPayload());
            actor.reply(msg, "World");
            latch.countDown();
        }

        @ResponseManually
        private void topic2(@ActorMessage ActorMsg msg) {
            Assert.assertEquals("Hello", msg.getPayload());
            actor.replyException(msg, new RuntimeException("exception msg"));
            latch.countDown();
        }

        public ReplyCls setActor(Actor actor) {
            this.actor = actor;
            return this;
        }
    }

    @Test
    public void testReply() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(4);
        ReplyCls cls = new ReplyCls(latch);
        Actor receiver = Actor.builder().addr("receiver")
                .setHandlerInstance(cls)
                .build();
        cls.setActor(receiver);
        Actor sender = Actor.builder().addr("sender").build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.sendThen("receiver", "topic1", "Hello")
                    .thenAccept(result -> {
                        Assert.assertEquals("World", result);
                        latch.countDown();
                    });
            sender.sendThen("receiver", "topic2","Hello")
                    .exceptionally(e -> {
                        Assert.assertEquals("exception msg", e.getMessage());
                        latch.countDown();
                        return null;
                    });
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));


    }
    @Test
    public void testSendThen() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addTopicHandlerFunction("topic", request -> {
            Assert.assertEquals("Hello!", request);
            latch.countDown();
            return "World!";
        }).build();
        Actor sender = Actor.builder().addr("sender").addResponseHandlerFunction("topic", resp -> {

            Assert.assertEquals("World!", resp.getResult());
            latch.countDown();
        }).build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "topic", "Hello!");
            Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));



    }

    @Test
    public void testResponseConfigIgnored() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addTopicHandlerFunction("topic", request -> {
            Assert.assertEquals("Hello!", request);
            latch.countDown();
            return "World";
        }).build();

        Actor sender = Actor.builder().addr("sender").addResponseHandlerFunction("topic", resp -> {
            // 响应配置为忽略，不应调用到这里
            latch.countDown();
        }).build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        sender.send("receiver", "topic", ActorMsg.Response.IGNORE, "Hello!");
        Assert.assertFalse(latch.await(1, TimeUnit.SECONDS));
        Assert.assertEquals(1, latch.getCount());
    }

    @Test
    public void testResponseConfigRequired() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addTopicHandlerFunction("topic", request -> {
            Assert.assertEquals("Hello!", request);
            latch.countDown();
        }).build();
        Actor sender = Actor.builder().addr("sender").addResponseHandlerFunction("topic", resp -> {

            Assert.assertNull(resp.getResult());
            latch.countDown();
        }).build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        sender.send("receiver", "topic", ActorMsg.Response.REQUIRED, "Hello!");
        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @SuppressWarnings("SameReturnValue")
    private static class ResponseManuallyCls {
        private Actor actor;
        @ActorListener
        @ResponseManually
        private String topic(@ActorMessage ActorMsg msg) {
            actor.reply(msg, "result1");
            return "result2";
        }

        public void setActor(Actor actor) {
            this.actor = actor;
        }
    }


    @Test
    public void testResponseManually() throws InterruptedException, ExecutionException {
        ResponseManuallyCls cls = new ResponseManuallyCls();
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(cls).build();
        cls.setActor(receiver);
        Actor sender = Actor.builder().addr("sender").addResponseHandlerFunction("topic", resp -> {

            Assert.assertEquals("result1", resp.getResult());
        }).build();
        PostOffice.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        String result = sender.<String>sendThen("receiver", "topic", ActorMsg.Response.REQUIRED, "Hello!").get();
        Assert.assertEquals("result1", result);

    }
}
