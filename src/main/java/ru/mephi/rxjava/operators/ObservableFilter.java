package ru.mephi.rxjava.operators;

import ru.mephi.rxjava.Observable;
import ru.mephi.rxjava.Observer;
import ru.mephi.rxjava.Predicate;
import ru.mephi.rxjava.internal.ConnectionObserver;

/**
 * Оператор filter — пропускает только элементы, удовлетворяющие предикату.
 */
public final class ObservableFilter<T> extends Observable<T> {

    private final Observable<T> source;
    private final Predicate<T> predicate;

    public ObservableFilter(Observable<T> source, Predicate<T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    protected void subscribeActual(Observer<T> observer) {
        FilterObserver<T> filterObserver = new FilterObserver<>(observer, predicate);
        source.subscribeUnsafe(filterObserver);
        ConnectionObserver.setUpstreamIfPossible(observer, filterObserver);
    }

    private static final class FilterObserver<T> extends ConnectionObserver<T> {

        private final Observer<T> downstream;
        private final Predicate<T> predicate;

        FilterObserver(Observer<T> downstream, Predicate<T> predicate) {
            this.downstream = downstream;
            this.predicate = predicate;
        }

        @Override
        public void onNext(T item) {
            if (isDisposed()) {
                return;
            }
            try {
                if (predicate.test(item)) {
                    downstream.onNext(item);
                }
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
