package io.journalkeeper.utils.actor;

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
    /**
     * true：方法参数为payload
     * false：方法参数为ActorMsg
     */
    boolean payload() default false;
    /** true：方法执行完成后发送响应消息，方法返回值作为响应消息，如果方法没有返回值，也会返回payload为null的响应消息
     * false：不返回响应
     */
    boolean response() default false;

    /** 是否消费pub/sub广播消息
     * true：消费pub/sub广播消息
     * false：只消费点对点发送的消息
     */
    boolean consumer() default false;
}
