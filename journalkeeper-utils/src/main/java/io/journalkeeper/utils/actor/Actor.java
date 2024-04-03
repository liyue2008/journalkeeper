package io.journalkeeper.utils.actor;

/**
 * Actor model
 */
public interface Actor {
    default String addr() {
        return this.getClass().getSimpleName();
    }

    /**
     * 接收消息
     * @param msg 消息
     * @return 是否成功接收消息
     */
    boolean receive(ActorMsg msg);
}
