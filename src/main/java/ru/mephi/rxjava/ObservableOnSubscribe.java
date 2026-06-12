package ru.mephi.rxjava;

/**
 * Источник данных для {@link Observable#create(ObservableOnSubscribe)}.
 */
@FunctionalInterface
public interface ObservableOnSubscribe<T> {

    void subscribe(ObservableEmitter<T> emitter) throws Exception;
}
