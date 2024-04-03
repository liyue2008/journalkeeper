package io.journalkeeper.utils.config;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Config {

    private final Map<String, Declaration> declarations = new HashMap<>();
    private final Map<String, Object> values = new ConcurrentHashMap<>();

    private final AtomicBoolean loaded = new AtomicBoolean(false);

    public <T> void declare(String key, Class<T> type , T defaultValue, boolean immutable, String description) {
        if (loaded.get()) {
            throw new IllegalStateException("Config is already loaded.");
        }
        declarations.put(key, new Declaration(key, type, defaultValue, immutable, description));
        values.put(key, defaultValue);
    }

    public void load(ConfigProvider configProvider) {
        if (loaded.compareAndSet(false, true)) {
            for (Declaration declaration : declarations.values()) {
                String key = declaration.getKey();
                Class<?> type = declaration.getType();
                String strValue = configProvider.get(key);
                if (strValue == null) {
                    continue;
                }
                Object value = parse(type, strValue);
                values.put(key, value);
            }
            configProvider.setListener(this::onConfigChange);
        } else {
            throw new IllegalStateException("Config is already loaded.");
        }
    }

    public void onConfigChange(String key, String strValue) {
        if (!loaded.get()) {
            return;
        }
        Declaration declaration = declarations.get(key);
        if (declaration == null  || declaration.isImmutable()) {
            return;
        }
        Class<?> type = declaration.getType();
        Object value = parse(type, strValue);
        values.put(key, value);
    }

    private Object parse(Class<?> type, String strValue) {
        if (type == String.class) {
            return strValue;
        }
        if (type == Integer.class) {
            return Integer.parseInt(strValue);
        }
        if (type == Long.class) {
            return Long.parseLong(strValue);
        }
        if (type == Boolean.class) {
            return Boolean.parseBoolean(strValue);
        }
        if (type == Double.class) {
            return Double.parseDouble(strValue);
        }
        if (type == Float.class) {
            return Float.parseFloat(strValue);
        }
        if (type == URI.class) {
            return URI.create(strValue);
        }

        if (type == Path.class) {
            return Paths.get(strValue);
        }
        throw new IllegalArgumentException("Unsupported type: " + type);

    }

    public <T> T get(String key) {
        if (!loaded.get()) {
            throw new IllegalStateException("Config is not loaded.");
        }
        //noinspection unchecked
        return (T) values.get(key);
    }

    private static class Declaration {
        private final String key;
        private final Class<?> type;
        private final Object defaultValue;


        private final boolean immutable;

        private final String description;


        public Declaration(String key, Class<?> type, Object defaultValue, boolean immutable, String description) {
            this.key = key;
            this.type = type;
            this.defaultValue = defaultValue;
            this.immutable = immutable;
            this.description = description;
        }

        public String getKey() {
            return key;
        }

        public Class getType() {
            return type;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }

        public boolean isImmutable() {
            return immutable;
        }

        public String getDescription() {
            return description;
        }
    }
}
