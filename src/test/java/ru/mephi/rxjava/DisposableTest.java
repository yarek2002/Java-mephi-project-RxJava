package ru.mephi.rxjava;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DisposableTest {

    @Test
    void disposePreventsFurtherEvents() throws Exception {
        List<Integer> values = new ArrayList<>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.dispose();
            emitter.onNext(2);
            emitter.onComplete();
        }).subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                values.add(item);
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        });

        assertEquals(List.of(1), values);
    }

    @Test
    void externalDisposeStopsStream() throws Exception {
        List<Integer> values = new ArrayList<>();
        Disposable[] disposableBox = new Disposable[1];

        disposableBox[0] = Observable.<Integer>create(emitter -> {
            for (int i = 1; i <= 5; i++) {
                if (emitter.isDisposed()) {
                    return;
                }
                emitter.onNext(i);
            }
            emitter.onComplete();
        }).subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer item) {
                values.add(item);
                if (item == 2) {
                    disposableBox[0].dispose();
                }
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onComplete() {
            }
        });

        assertTrue(disposableBox[0].isDisposed());
        assertEquals(List.of(1, 2), values);
    }
}
