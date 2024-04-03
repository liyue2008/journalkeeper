package io.journalkeeper.utils.config;

import java.util.Properties;

public class PropertiesConfigProvider  implements ConfigProvider {
    private final Properties properties;

    public PropertiesConfigProvider(Properties properties) {
        this.properties = properties;
    }


    @Override
    public String get(String key) {
        return properties.getProperty(key);
    }
}
