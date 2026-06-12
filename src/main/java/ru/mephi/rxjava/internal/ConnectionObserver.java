package ru.mephi.rxjava.internal;

import ru.mephi.rxjava.Disposable;
import ru.mephi.rxjava.Observer;

/**
 * Базовый наблюдатель с поддержкой отмены upstream-подписки.
 */
public abstract class ConnectionObserver<T> implements Observer<T>, Disposable {

    protected final SerialDisposable upstream = new SerialDisposable();
    protected volatile boolean disposed;

    public void setUpstream(Disposable disposable) {
        upstream.set(disposable);
    }

    @Override
    public void dispose() {
        disposed = true;
        upstream.dispose();
    }

    @Override
    public boolean isDisposed() {
        return disposed;
    }

    public static void setUpstreamIfPossible(Observer<?> observer, Disposable upstream) {
        if (observer instanceof SafeObserver<?> safeObserver) {
            safeObserver.setUpstream(upstream);
        }
        if (observer instanceof ConnectionObserver<?> connectionObserver) {
            connectionObserver.setUpstream(upstream);
        }
    }
}
