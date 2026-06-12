package ru.mephi.rxjava;

/**
 * Функция преобразования значения.
 */
@FunctionalInterface
public interface Function<T, R> {

    R apply(T value) throws Exception;
}
