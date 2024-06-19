package io.journalkeeper.utils.net;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author liyue
 */
public class StickySession <T>{
    private final List<T> remotes;
    private T session;
    public StickySession(List<T> remotes) {
        this.remotes = Collections.unmodifiableList(remotes);
        session = remotes.get(ThreadLocalRandom.current().nextInt(remotes.size()));
    }

    public T getSession() {
        return session;
    }

    public T rebind() {
        T newSession = session;
        if (remotes.size() > 1) {
            while (Objects.equals(newSession, session)) {
                newSession = remotes.get(ThreadLocalRandom.current().nextInt(remotes.size()));
            }
        }
        return session = newSession;
    }
}
