package ru.mephi.rxjava.internal;

import ru.mephi.rxjava.Disposable;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Потокобезопасная ссылка на текущую подписку.
 */
public final class SerialDisposable implements Disposable {

    private final AtomicReference<Disposable> resource = new AtomicReference<>();

    @Override
    public void dispose() {
        Disposable current = resource.getAndSet(DisposableHelper.DISPOSED);
        DisposableHelper.dispose(current);
    }

    @Override
    public boolean isDisposed() {
        return DisposableHelper.isDisposed(resource.get());
    }

    public void set(Disposable disposable) {
        Disposable current = resource.get();
        if (current == DisposableHelper.DISPOSED) {
            DisposableHelper.dispose(disposable);
            return;
        }
        if (resource.compareAndSet(current, disposable) && current != null) {
            current.dispose();
        }
    }

    public Disposable get() {
        Disposable current = resource.get();
        return current == DisposableHelper.DISPOSED ? null : current;
    }
}
