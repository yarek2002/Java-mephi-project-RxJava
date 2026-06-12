package ru.mephi.rxjava.operators;

import ru.mephi.rxjava.Function;
import ru.mephi.rxjava.Observable;
import ru.mephi.rxjava.Observer;
import ru.mephi.rxjava.internal.ConnectionObserver;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Оператор flatMap — преобразует элементы в новые Observable и объединяет их эмиссии.
 */
public final class ObservableFlatMap<T, R> extends Observable<R> {

    private final Observable<T> source;
    private final Function<T, Observable<R>> mapper;

    public ObservableFlatMap(Observable<T> source, Function<T, Observable<R>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected void subscribeActual(Observer<R> observer) {
        FlatMapObserver<T, R> flatMapObserver = new FlatMapObserver<>(observer, mapper);
        source.subscribeUnsafe(flatMapObserver);
        ConnectionObserver.setUpstreamIfPossible(observer, flatMapObserver);
    }

    private static final class FlatMapObserver<T, R> extends ConnectionObserver<T> {

        private final Observer<R> downstream;
        private final Function<T, Observable<R>> mapper;
        private final AtomicInteger activeCount = new AtomicInteger(0);
        private volatile boolean sourceCompleted;

        FlatMapObserver(Observer<R> downstream, Function<T, Observable<R>> mapper) {
            this.downstream = downstream;
            this.mapper = mapper;
        }

        @Override
        public void onNext(T item) {
            if (isDisposed()) {
                return;
            }
            Observable<R> inner;
            try {
                inner = mapper.apply(item);
                if (inner == null) {
                    onError(new NullPointerException("flatMap mapper returned null Observable"));
                    return;
                }
            } catch (Throwable error) {
                onError(error);
                return;
            }

            activeCount.incrementAndGet();
            inner.subscribeUnsafe(new InnerObserver());
        }

        @Override
        public void onError(Throwable t) {
            if (!isDisposed()) {
                dispose();
                downstream.onError(t);
            }
        }

        @Override
        public void onComplete() {
            sourceCompleted = true;
            completeIfDone();
        }

        private void completeIfDone() {
            if (sourceCompleted && activeCount.get() == 0 && !isDisposed()) {
                downstream.onComplete();
            }
        }

        private final class InnerObserver implements Observer<R> {

            @Override
            public void onNext(R item) {
                if (!isDisposed()) {
                    downstream.onNext(item);
                }
            }

            @Override
            public void onError(Throwable t) {
                FlatMapObserver.this.onError(t);
            }

            @Override
            public void onComplete() {
                if (activeCount.decrementAndGet() == 0) {
                    completeIfDone();
                }
            }
        }
    }
}
