package ru.mephi.rxjava;

/**
 * Подписка, которую можно отменить.
 */
public interface Disposable {

    void dispose();

    boolean isDisposed();
}
