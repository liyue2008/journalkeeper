package io.journalkeeper.core.easy;

import java.io.Serializable;

public class JkRequest implements Serializable {

    private static final long serialVersionUID = -6954103855717561471L;
    private final String command;

    private final Object parameter;

    public JkRequest(String command, Object parameter) {
        this.command = command;
        this.parameter = parameter;
    }

    public String getCommand() {
        return command;
    }

    public <T> T getParameter() {
        //noinspection unchecked
        return (T) parameter;
    }
}
