package io.journalkeeper.coordinating.state.domain;

import java.io.Serializable;

public class CoordinatingEvent implements Serializable {

    private final static long serialVersionUID = -3090672969964499328L;
    private final String type;
    private final String key;
    private final String value;

    public CoordinatingEvent(String type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
