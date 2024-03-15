package io.journalkeeper.core.easy;

import io.journalkeeper.core.serialize.WrappedStateFactory;

public interface JkStateFactory extends WrappedStateFactory<JkRequest, JkResponse, JkRequest, JkResponse> {
    JkState createState();
}
