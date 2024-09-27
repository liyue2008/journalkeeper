package io.journalkeeper.utils;

import java.io.Closeable;
import java.io.IOException;

public class CloseSupport {

    public static void tryClose(Object obj) {
        if (null != obj && obj instanceof Closeable) {
            try {
                ((Closeable)obj).close();
            } catch (IOException ignored) {
            }
        }
    }
}
