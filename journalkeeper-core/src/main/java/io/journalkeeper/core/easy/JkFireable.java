package io.journalkeeper.core.easy;


import java.util.Map;

public interface JkFireable {

    void fireEvent(Map<String, String> event);
}
