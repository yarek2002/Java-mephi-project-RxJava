package ru.mephi.rxjava.operators;

import ru.mephi.rxjava.Observable;
import ru.mephi.rxjava.ObservableOnSubscribe;
import ru.mephi.rxjava.Observer;
import ru.mephi.rxjava.internal.ConnectionObserver;
import ru.mephi.rxjava.internal.CreateEmitter;

/**
 * Observable, созданный через фабричный метод create().
 */
public final class ObservableCreate<T> extends Observable<T> {

    private final ObservableOnSubscribe<T> onSubscribe;

    public ObservableCreate(ObservableOnSubscribe<T> onSubscribe) {
        this.onSubscribe = onSubscribe;
    }

    @Override
    protected void subscribeActual(Observer<T> observer) {
        CreateEmitter<T> emitter = new CreateEmitter<>(observer);
        ConnectionObserver.setUpstreamIfPossible(observer, emitter);

        try {
            onSubscribe.subscribe(emitter);
        } catch (Throwable error) {
            emitter.onError(error);
        }
    }
}
