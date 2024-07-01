package io.journalkeeper.utils.actor;

import java.text.SimpleDateFormat;

public class ActorMetric {
    private static final ThreadLocal<SimpleDateFormat> sdfHolder
            = ThreadLocal.withInitial(() -> new SimpleDateFormat("HH:mm:ss.SSS"));

    private final long createTime = System.currentTimeMillis(); // 创建时间
    private long outboxEnqueueTime;
    private long outboxDequeueTime;
    private long inboxEnqueueTime;
    private long inboxDequeueTime;
    private long consumedTime; // 消费完成的时间

    private int inboxEnqueueSize;
    private int inboxDequeueSize;
    private String inboxQueueName;
    private int outBoxEnqueueSize;
    private int outBoxDequeueSize;
    private String outBoxQueueName;

    public long getCreateTime() {
        return createTime;
    }

    public long getInboxEnqueueTime() {
        return inboxEnqueueTime;
    }

    void onInboxEnqueue(String queueName, int queueSize) {
        this.inboxQueueName = queueName;
        this.inboxEnqueueSize = queueSize;
        this.inboxEnqueueTime = System.currentTimeMillis();
    }
    void onInboxDequeue(int queueSize) {
        this.inboxDequeueSize = queueSize;
        this.inboxDequeueTime = System.currentTimeMillis();
    }

    public long getOutboxEnqueueTime() {
        return outboxEnqueueTime;
    }

    void onOutboxEnqueue(String queueName, int queueSize) {
        this.outBoxQueueName = queueName;
        this.outBoxEnqueueSize = queueSize;
        this.outboxEnqueueTime = System.currentTimeMillis();
    }
    void onOutboxDequeue(int queueSize) {
        this.outBoxDequeueSize = queueSize;
        this.outboxDequeueTime = System.currentTimeMillis();
    }

    public long getInboxDequeueTime() {
        return inboxDequeueTime;
    }

    public long getConsumedTime() {
        return consumedTime;
    }

    void onConsumed() {
        this.consumedTime = System.currentTimeMillis();
    }

    public int getInboxEnqueueSize() {
        return inboxEnqueueSize;
    }

    public String getInboxQueueName() {
        return inboxQueueName;
    }

    public int getOutBoxEnqueueSize() {
        return outBoxEnqueueSize;
    }

    public String getOutBoxQueueName() {
        return outBoxQueueName;
    }

    public long getOutboxDequeueTime() {
        return outboxDequeueTime;
    }

    public int getInboxDequeueSize() {
        return inboxDequeueSize;
    }

    public int getOutBoxDequeueSize() {
        return outBoxDequeueSize;
    }

    public long cost() {
        return consumedTime - createTime;
    }
    @Override
    public String toString() {
        return "{" +
                "[" + sdfHolder.get().format(createTime) +
                "]--(+" + (outboxEnqueueTime - createTime) + "ms | " + outBoxEnqueueSize +
                ")-->[" + outBoxQueueName + "]--(+" + (outboxDequeueTime - outboxEnqueueTime) + "ms | " + outBoxDequeueSize + ")--(+"
                + (inboxEnqueueTime - outboxDequeueTime) + "ms | " + inboxEnqueueSize + ")-->[" + inboxQueueName + "]--(+"
                + (inboxDequeueTime - inboxEnqueueTime) + "ms | " + inboxDequeueSize + ")-->[Consumer]--(+"
                 + (consumedTime - inboxDequeueTime) + "ms)-->[" + sdfHolder.get().format(consumedTime) +
                "]}";
    }
}
