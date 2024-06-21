package io.journalkeeper.utils.actor;

import java.text.SimpleDateFormat;

public class ActorMetric {
    private static final ThreadLocal<SimpleDateFormat> sdfHolder
            = ThreadLocal.withInitial(() -> new SimpleDateFormat("HH:mm:ss.SSS"));

    private final long createTime = System.currentTimeMillis(); // 创建时间
    private long inboxTime; // 放入 sender inbox 的时间
    private long outboxTime; // 放入 receiver outbox 的时间
    private long deliverTime; // 从 receiver outbox 中取出的时间
    private long consumedTime; // 消费完成的时间

    private int inboxQueueSize;
    private String inboxQueueName;
    private int outBoxQueueSize;
    private String outBoxQueueName;

    public long getCreateTime() {
        return createTime;
    }

    public long getInboxTime() {
        return inboxTime;
    }

    void onInboxIn(String queueName, int queueSize) {
        this.inboxQueueName = queueName;
        this.inboxQueueSize = queueSize;
        this.inboxTime = System.currentTimeMillis();
    }

    public long getOutboxTime() {
        return outboxTime;
    }

    void onOutboxIn(String queueName, int queueSize) {
        this.outBoxQueueName = queueName;
        this.outBoxQueueSize = queueSize;
        this.outboxTime = System.currentTimeMillis();
    }

    public long getDeliverTime() {
        return deliverTime;
    }

    void onDeliver() {
        this.deliverTime = System.currentTimeMillis();
    }

    public long getConsumedTime() {
        return consumedTime;
    }

    void onConsumed() {
        this.consumedTime = System.currentTimeMillis();
    }

    public int getInboxQueueSize() {
        return inboxQueueSize;
    }

    public String getInboxQueueName() {
        return inboxQueueName;
    }

    public int getOutBoxQueueSize() {
        return outBoxQueueSize;
    }

    public String getOutBoxQueueName() {
        return outBoxQueueName;
    }

    public long cost() {
        return consumedTime - createTime;
    }
    @Override
    public String toString() {
        return "ActorMetric{" +
                "createTime=" + sdfHolder.get().format(createTime) +
                ", outboxTime=(+" + (outboxTime - createTime) + ") queSize=" + outBoxQueueSize +
                ", inboxTime=(+" + (inboxTime - outboxTime) + ") queSize=" + inboxQueueSize +
                ", deliverTime=(+" + (deliverTime - inboxTime) + ")" +
                ", consumedTime=(+" + (consumedTime - deliverTime) + ")" +
                '}';
    }
}
