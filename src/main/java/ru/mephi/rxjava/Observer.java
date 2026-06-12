package ru.mephi.rxjava;

/**
 * Наблюдатель реактивного потока.
 */
public interface Observer<T> {

    void onNext(T item);

    void onError(Throwable t);

    void onComplete();
}
