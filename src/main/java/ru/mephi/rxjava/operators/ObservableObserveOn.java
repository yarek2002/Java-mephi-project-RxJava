package ru.mephi.rxjava.operators;

import ru.mephi.rxjava.Observable;
import ru.mephi.rxjava.Observer;
import ru.mephi.rxjava.Scheduler;
import ru.mephi.rxjava.internal.ConnectionObserver;

/**
 * Оператор observeOn — доставка событий наблюдателю выполняется в заданном Scheduler.
 */
public final class ObservableObserveOn<T> extends Observable<T> {

    private final Observable<T> source;
    private final Scheduler scheduler;

    public ObservableObserveOn(Observable<T> source, Scheduler scheduler) {
        this.source = source;
        this.scheduler = scheduler;
    }

    @Override
    protected void subscribeActual(Observer<T> observer) {
        ObserveOnObserver<T> observeOnObserver = new ObserveOnObserver<>(observer, scheduler);
        source.subscribeUnsafe(observeOnObserver);
        ConnectionObserver.setUpstreamIfPossible(observer, observeOnObserver);
    }

    private static final class ObserveOnObserver<T> extends ConnectionObserver<T> {

        private final Observer<T> downstream;
        private final Scheduler scheduler;

        ObserveOnObserver(Observer<T> downstream, Scheduler scheduler) {
            this.downstream = downstream;
            this.scheduler = scheduler;
        }

        @Override
        public void onNext(T item) {
            schedule(() -> {
                if (!isDisposed()) {
                    downstream.onNext(item);
                }
            });
        }

        @Override
        public void onError(Throwable t) {
            schedule(() -> {
                if (!isDisposed()) {
                    downstream.onError(t);
                }
            });
        }

        @Override
        public void onComplete() {
            schedule(() -> {
                if (!isDisposed()) {
                    downstream.onComplete();
                }
            });
        }

        private void schedule(Runnable action) {
            if (!isDisposed()) {
                scheduler.execute(action);
            }
        }
    }
}
