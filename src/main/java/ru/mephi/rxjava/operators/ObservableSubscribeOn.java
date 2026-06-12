package ru.mephi.rxjava.operators;

import ru.mephi.rxjava.Disposable;
import ru.mephi.rxjava.Observable;
import ru.mephi.rxjava.Observer;
import ru.mephi.rxjava.Scheduler;
import ru.mephi.rxjava.internal.ConnectionObserver;

/**
 * Оператор subscribeOn — подписка на источник выполняется в заданном Scheduler.
 */
public final class ObservableSubscribeOn<T> extends Observable<T> {

    private final Observable<T> source;
    private final Scheduler scheduler;

    public ObservableSubscribeOn(Observable<T> source, Scheduler scheduler) {
        this.source = source;
        this.scheduler = scheduler;
    }

    @Override
    protected void subscribeActual(Observer<T> observer) {
        SubscribeOnObserver<T> subscribeOnObserver = new SubscribeOnObserver<>(source, observer);
        ConnectionObserver.setUpstreamIfPossible(observer, subscribeOnObserver);
        scheduler.execute(subscribeOnObserver);
    }

    private static final class SubscribeOnObserver<T> extends ConnectionObserver<T> implements Runnable {

        private final Observable<T> source;
        private final Observer<T> downstream;

        SubscribeOnObserver(Observable<T> source, Observer<T> downstream) {
            this.source = source;
            this.downstream = downstream;
        }

        @Override
        public void run() {
            if (!isDisposed()) {
                source.subscribeUnsafe(this);
            }
        }

        @Override
        public void onNext(T item) {
            downstream.onNext(item);
        }

        @Override
        public void onError(Throwable t) {
            downstream.onError(t);
        }

        @Override
        public void onComplete() {
            downstream.onComplete();
        }
    }
}
