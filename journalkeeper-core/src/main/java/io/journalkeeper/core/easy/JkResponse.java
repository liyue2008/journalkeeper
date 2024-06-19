package io.journalkeeper.core.easy;

import java.io.Serializable;

class JkResponse implements Serializable {

    private static final long serialVersionUID = 1899023334612352269L;
    private final String command;
    private final Object result;

    private final Exception exception;


    public JkResponse(String command, Object result) {
        this.command = command;
        this.result = result;
        this.exception = null;
    }

    public JkResponse(String command, Exception e) {
        this.command = command;
        this.result = null;
        this.exception = e;
    }

    public String getCommand() {
        return command;
    }

    @SuppressWarnings("unchecked")
    public <T> T getResult() {
        return (T) result;
    }

    public Exception getException() {
        return exception;
    }

    boolean isSuccess() {
        return exception == null;
    }
}
