package ru.mephi.rxjava.operators;

import ru.mephi.rxjava.Disposable;
import ru.mephi.rxjava.Function;
import ru.mephi.rxjava.Observable;
import ru.mephi.rxjava.Observer;
import ru.mephi.rxjava.internal.ConnectionObserver;

/**
 * Оператор map — преобразует каждый элемент потока.
 */
public final class ObservableMap<T, R> extends Observable<R> {

    private final Observable<T> source;
    private final Function<T, R> mapper;

    public ObservableMap(Observable<T> source, Function<T, R> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    protected void subscribeActual(Observer<R> observer) {
        MapObserver<T, R> mapObserver = new MapObserver<>(observer, mapper);
        source.subscribeUnsafe(mapObserver);
        ConnectionObserver.setUpstreamIfPossible(observer, mapObserver);
    }

    private static final class MapObserver<T, R> extends ConnectionObserver<T> {

        private final Observer<R> downstream;
        private final Function<T, R> mapper;

        MapObserver(Observer<R> downstream, Function<T, R> mapper) {
            this.downstream = downstream;
            this.mapper = mapper;
        }

        @Override
        public void onNext(T item) {
            if (isDisposed()) {
                return;
            }
            try {
                R result = mapper.apply(item);
                if (result == null) {
                    onError(new NullPointerException("Mapper returned null"));
                    return;
                }
                downstream.onNext(result);
            } catch (Throwable error) {
                onError(error);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (!isDisposed()) {
                downstream.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (!isDisposed()) {
                downstream.onComplete();
            }
        }
    }
}
