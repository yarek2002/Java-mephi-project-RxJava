package ru.mephi.rxjava.internal;

import ru.mephi.rxjava.Disposable;

/**
 * Вспомогательные методы для работы с Disposable.
 */
public final class DisposableHelper {

    public static final Disposable DISPOSED = new Disposable() {
        @Override
        public void dispose() {
        }

        @Override
        public boolean isDisposed() {
            return true;
        }
    };

    private DisposableHelper() {
    }

    public static boolean isDisposed(Disposable disposable) {
        return disposable != null && disposable.isDisposed();
    }

    public static boolean dispose(Disposable disposable) {
        if (disposable == null) {
            return false;
        }
        Disposable current = disposable;
        if (current.isDisposed()) {
            return false;
        }
        current.dispose();
        return true;
    }
}
