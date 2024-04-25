package io.journalkeeper.utils.actor.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ActorListener {
    /**
     * 消息主题，如果为空采用方法名作为主题
     */
    String topic() default "";
}
