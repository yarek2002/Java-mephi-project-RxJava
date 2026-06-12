package ru.mephi.rxjava;

/**
 * Предикат для фильтрации элементов потока.
 */
@FunctionalInterface
public interface Predicate<T> {

    boolean test(T value) throws Exception;
}
