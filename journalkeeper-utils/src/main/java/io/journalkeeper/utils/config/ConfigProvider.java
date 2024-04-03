package io.journalkeeper.utils.config;

import java.util.function.BiConsumer;

public interface ConfigProvider {

    String get(String key);

    default void setListener(BiConsumer<String, String> listener) {
        // nothing to do
    }
}
