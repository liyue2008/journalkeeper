package io.journalkeeper.core.persistence;

/**
 * @author LiYue
 * Date: 2020/5/13
 */
public class StringPersistenceID implements PersistenceID {
    private final String id;

    public StringPersistenceID(String id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StringPersistenceID that = (StringPersistenceID) o;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
