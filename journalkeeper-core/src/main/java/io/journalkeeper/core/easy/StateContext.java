package io.journalkeeper.core.easy;


import java.nio.file.Path;
import java.util.Properties;

public interface StateContext {

    void fireEvent(Object event);
    Path getStatePath();
    Properties getProperties();
}
