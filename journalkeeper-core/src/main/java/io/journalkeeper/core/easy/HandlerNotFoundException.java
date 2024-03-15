package io.journalkeeper.core.easy;

public class HandlerNotFoundException  extends RuntimeException{
    private final String command;
    public HandlerNotFoundException(String command) {
        this.command = command;

    }

    @Override
    public String getMessage() {
        return String.format("Handler not found for command: %s", this.command);
    }
}
