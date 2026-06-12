package ru.mephi.rxjava;

/**
 * Эмиттер событий при создании Observable через {@link Observable#create(ObservableOnSubscribe)}.
 */
public interface ObservableEmitter<T> {

    void onNext(T value);

    void onError(Throwable error);

    void onComplete();

    boolean isDisposed();

    void dispose();
}
