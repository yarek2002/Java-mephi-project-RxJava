package ru.mephi.rxjava;

import ru.mephi.rxjava.internal.SafeObserver;
import ru.mephi.rxjava.operators.ObservableCreate;
import ru.mephi.rxjava.operators.ObservableFilter;
import ru.mephi.rxjava.operators.ObservableFlatMap;
import ru.mephi.rxjava.operators.ObservableMap;
import ru.mephi.rxjava.operators.ObservableObserveOn;
import ru.mephi.rxjava.operators.ObservableSubscribeOn;

import java.util.Objects;

/**
 * Реактивный поток данных с поддержкой операторов преобразования и планировщиков.
 */
public abstract class Observable<T> {

    public static <T> Observable<T> create(ObservableOnSubscribe<T> onSubscribe) {
        Objects.requireNonNull(onSubscribe, "onSubscribe is null");
        return new ObservableCreate<>(onSubscribe);
    }

    public final Disposable subscribe(Observer<T> observer) {
        Objects.requireNonNull(observer, "observer is null");
        SafeObserver<T> safeObserver = new SafeObserver<>(observer);
        subscribeUnsafe(safeObserver);
        return safeObserver;
    }

    public final void subscribeUnsafe(Observer<T> observer) {
        try {
            subscribeActual(observer);
        } catch (Throwable error) {
            observer.onError(error);
        }
    }

    protected abstract void subscribeActual(Observer<T> observer);

    public final <R> Observable<R> map(Function<T, R> mapper) {
        Objects.requireNonNull(mapper, "mapper is null");
        return new ObservableMap<>(this, mapper);
    }

    public final Observable<T> filter(Predicate<T> predicate) {
        Objects.requireNonNull(predicate, "predicate is null");
        return new ObservableFilter<>(this, predicate);
    }

    public final <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {
        Objects.requireNonNull(mapper, "mapper is null");
        return new ObservableFlatMap<>(this, mapper);
    }

    public final Observable<T> subscribeOn(Scheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler is null");
        return new ObservableSubscribeOn<>(this, scheduler);
    }

    public final Observable<T> observeOn(Scheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler is null");
        return new ObservableObserveOn<>(this, scheduler);
    }
}
