package ru.mephi.rxjava;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class OperatorTest {

    @Test
    void mapTransformsValues() throws Exception {
        List<String> result = collect(
                Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onComplete();
                }).map(value -> "n=" + value)
        );

        assertEquals(List.of("n=1", "n=2"), result);
    }

    @Test
    void filterKeepsMatchingValues() throws Exception {
        List<Integer> result = collect(
                Observable.<Integer>create(emitter -> {
                    emitter.onNext(1);
                    emitter.onNext(2);
                    emitter.onNext(3);
                    emitter.onComplete();
                }).filter(value -> value % 2 == 0)
        );

        assertEquals(List.of(2), result);
    }

    @Test
    void flatMapMergesInnerStreams() throws Exception {
        List<String> result = collect(
                Observable.<String>create(emitter -> {
                    emitter.onNext("a");
                    emitter.onNext("b");
                    emitter.onComplete();
                }).flatMap(letter -> Observable.<String>create(inner -> {
                    inner.onNext(letter + "1");
                    inner.onNext(letter + "2");
                    inner.onComplete();
                }))
        );

        assertEquals(4, result.size());
        assertTrue(result.contains("a1"));
        assertTrue(result.contains("a2"));
        assertTrue(result.contains("b1"));
        assertTrue(result.contains("b2"));
    }

    @Test
    void mapErrorIsDeliveredToOnError() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        }).map(value -> {
            throw new IllegalStateException("map failed");
        }).subscribe(new Observer<Object>() {
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
        assertInstanceOf(IllegalStateException.class, error.get());
    }

    private static <T> List<T> collect(Observable<T> observable) throws InterruptedException {
        List<T> values = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        observable.subscribe(new Observer<T>() {
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
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        return values;
    }
}
