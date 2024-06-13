package io.journalkeeper.utils.actor;

class ActorThreadContext {
    private final boolean isPostmanThread;

    public ActorThreadContext(boolean isPostmanThread) {
        this.isPostmanThread = isPostmanThread;
    }

    public boolean isPostmanThread() {
        return isPostmanThread;
    }
}
