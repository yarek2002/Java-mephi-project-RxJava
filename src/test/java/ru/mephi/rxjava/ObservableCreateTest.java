package ru.mephi.rxjava;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ObservableCreateTest {

    @Test
    void createEmitsItemsAndCompletes() throws Exception {
        List<Integer> values = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        }).subscribe(new TestObserver<>(values, latch));

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertEquals(List.of(1, 2, 3), values);
    }

    @Test
    void createPropagatesError() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        RuntimeException failure = new RuntimeException("boom");

        Observable.create(emitter -> emitter.onError(failure))
                .subscribe(new Observer<Object>() {
                    @Override
                    public void onNext(Object item) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        error.set(t);
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                    }
                });

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        assertSame(failure, error.get());
    }

    @Test
    void disposeStopsEmissions() throws Exception {
        List<Integer> values = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        Disposable disposable = Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        }).subscribe(new TestObserver<>(values, latch));

        disposable.dispose();
        assertTrue(disposable.isDisposed());
    }

    private static final class TestObserver<T> implements Observer<T> {

        private final List<T> values;
        private final CountDownLatch latch;

        TestObserver(List<T> values, CountDownLatch latch) {
            this.values = values;
            this.latch = latch;
        }

        @Override
        public void onNext(T item) {
            values.add(item);
        }

        @Override
        public void onError(Throwable t) {
            latch.countDown();
        }

        @Override
        public void onComplete() {
            latch.countDown();
        }
    }
}
