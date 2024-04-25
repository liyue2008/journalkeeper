package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.actor.annotation.ActorResponseListener;
import io.journalkeeper.utils.actor.annotation.ActorScheduler;
import io.journalkeeper.utils.actor.annotation.ActorSubscriber;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

class ActorUtils {

    static Map<String, Method> scanActionListeners(Object instance, Class<? extends Annotation> annotation) {
        return scanActionListeners(instance, Collections.singleton(annotation));
    }
    static Map<String, Method> scanActionListeners(Object instance, Collection<Class<? extends Annotation>> annotations) {
        return Arrays.stream(instance.getClass().getDeclaredMethods())
                .filter(method -> annotations.stream().anyMatch(method::isAnnotationPresent))
                .collect(Collectors.toMap(method -> methodToTopic(method, annotations), method -> method));
    }

    static String methodToTopic(Method method, Class<? extends Annotation> annotation) {
        return methodToTopic(method, Collections.singleton(annotation));
    }
    static String methodToTopic(Method method, Collection<Class<? extends Annotation>> annotations) {
        String topic = "";
        if (annotations.contains(ActorListener.class) && method.isAnnotationPresent(ActorListener.class)) {
            topic = method.getAnnotation(ActorListener.class).topic();
        }
        if (annotations.contains(ActorScheduler.class) && method.isAnnotationPresent(ActorScheduler.class)) {
            topic = method.getAnnotation(ActorScheduler.class).topic();
        }
        if (annotations.contains(ActorSubscriber.class) && method.isAnnotationPresent(ActorSubscriber.class)) {
            topic = method.getAnnotation(ActorSubscriber.class).topic();
        }
        if (annotations.contains(ActorResponseListener.class) && method.isAnnotationPresent(ActorResponseListener.class)) {
            topic = method.getAnnotation(ActorResponseListener.class).topic();
        }
        // 如果topic为空，则使用方法名作为topic
        if (topic.isEmpty()) {
            topic = method.getName();
        }
        return topic;
    }
}
