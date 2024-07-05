package io.journalkeeper.utils.actor;

import io.journalkeeper.utils.actor.annotation.ActorListener;
import io.journalkeeper.utils.actor.annotation.ActorResponseListener;
import io.journalkeeper.utils.actor.annotation.ActorScheduler;
import io.journalkeeper.utils.actor.annotation.ActorSubscriber;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;

class ActorUtils {

    static Map<String, List<InvocationTarget>> scanActionListeners(Object instance, Class<? extends Annotation> annotation) {
        return scanActionListeners(instance, Collections.singleton(annotation));
    }
    static Map<String, List<InvocationTarget>> scanActionListeners(Object instance, Collection<Class<? extends Annotation>> annotations) {
        return Arrays.stream(instance.getClass().getDeclaredMethods())
                .filter(method -> annotations.stream().anyMatch(method::isAnnotationPresent))
                .reduce(new HashMap<>(), (map, method) -> {
                    String topic = methodToTopic(method, annotations);
                    List<InvocationTarget> list = map.computeIfAbsent(topic, t -> new LinkedList<>());
                    list.add(new InvocationTarget(instance, method));
                    return map;
                }, ActorUtils::mergeInvocationTargetMap);
    }

    /**
     * 将sourceMap合并到targetMap，返回targetMap
     */
    static Map<String, List<InvocationTarget>> mergeInvocationTargetMap(Map<String, List<InvocationTarget>> targetMap, Map<String, List<InvocationTarget>> sourceMap) {
        sourceMap.forEach((key, value) -> {
            List<InvocationTarget> list = targetMap.computeIfAbsent(key, k -> new LinkedList<>());
            list.addAll(value);
        });
        return targetMap;
    }

    static String methodToTopic(Method method) {
        String topic = "";
        if (method.isAnnotationPresent(ActorListener.class)){
            topic = method.getAnnotation(ActorListener.class).topic();
        } else if (method.isAnnotationPresent(ActorScheduler.class)) {
            topic = method.getAnnotation(ActorScheduler.class).topic();
        } else if (method.isAnnotationPresent(ActorSubscriber.class)) {
            topic = method.getAnnotation(ActorSubscriber.class).topic();
        } else if (method.isAnnotationPresent(ActorResponseListener.class)) {
            topic = method.getAnnotation(ActorResponseListener.class).topic();
        }
        // 如果topic为空，则使用方法名作为topic
        if (topic.isEmpty()) {
            topic = method.getName();
        }
        return topic;
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
