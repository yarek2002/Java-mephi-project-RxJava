package ru.mephi.rxjava.internal;

import ru.mephi.rxjava.Disposable;
import ru.mephi.rxjava.ObservableEmitter;
import ru.mephi.rxjava.Observer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Эмиттер для Observable.create().
 */
public final class CreateEmitter<T> extends AtomicBoolean implements ObservableEmitter<T>, Disposable {

    private final Observer<T> observer;
    private final SerialDisposable resource = new SerialDisposable();

    public CreateEmitter(Observer<T> observer) {
        this.observer = observer;
    }

    @Override
    public void onNext(T value) {
        if (isDisposed()) {
            return;
        }
        if (value == null) {
            onError(new NullPointerException("onNext called with null value"));
            return;
        }
        observer.onNext(value);
    }

    @Override
    public void onError(Throwable error) {
        if (!compareAndSet(false, true)) {
            return;
        }
        resource.dispose();
        if (error == null) {
            error = new NullPointerException("onError called with null throwable");
        }
        observer.onError(error);
    }

    @Override
    public void onComplete() {
        if (!compareAndSet(false, true)) {
            return;
        }
        resource.dispose();
        observer.onComplete();
    }

    @Override
    public boolean isDisposed() {
        return get();
    }

    @Override
    public void dispose() {
        if (compareAndSet(false, true)) {
            resource.dispose();
        }
    }
}
