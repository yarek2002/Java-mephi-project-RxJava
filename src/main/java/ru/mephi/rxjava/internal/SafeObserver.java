package ru.mephi.rxjava.internal;

import ru.mephi.rxjava.Disposable;
import ru.mephi.rxjava.Observer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Обёртка над Observer с защитой от повторных терминальных событий.
 */
public final class SafeObserver<T> implements Observer<T>, Disposable {

    private final Observer<T> downstream;
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private final SerialDisposable upstream = new SerialDisposable();

    public SafeObserver(Observer<T> downstream) {
        this.downstream = downstream;
    }

    @Override
    public void onNext(T item) {
        if (isTerminatedOrDisposed()) {
            return;
        }
        if (item == null) {
            onError(new NullPointerException("onNext called with null value"));
            return;
        }
        downstream.onNext(item);
    }

    @Override
    public void onError(Throwable t) {
        if (!terminated.compareAndSet(false, true)) {
            return;
        }
        dispose();
        if (t == null) {
            t = new NullPointerException("onError called with null throwable");
        }
        downstream.onError(t);
    }

    @Override
    public void onComplete() {
        if (!terminated.compareAndSet(false, true)) {
            return;
        }
        dispose();
        downstream.onComplete();
    }

    @Override
    public void dispose() {
        disposed.set(true);
        upstream.dispose();
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }

    public void setUpstream(Disposable disposable) {
        upstream.set(disposable);
    }

    private boolean isTerminatedOrDisposed() {
        return terminated.get() || disposed.get();
    }
}
