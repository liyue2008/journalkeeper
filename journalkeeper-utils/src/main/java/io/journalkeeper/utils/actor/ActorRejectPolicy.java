package io.journalkeeper.utils.actor;

/**
 * @author LiYue
 * Date: 2019-09-19
 * 发送时队列满的拒绝策略
 */
public enum ActorRejectPolicy {
    EXCEPTION, // 抛出异常
    BLOCK, // 阻塞
    DROP // 丢弃并返回NULL
}
