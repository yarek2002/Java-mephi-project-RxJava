package ru.mephi.rxjava;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class SchedulerTest {

    @Test
    void subscribeOnRunsSubscriptionOnIoScheduler() throws Exception {
        AtomicReference<String> subscribeThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            subscribeThread.set(Thread.currentThread().getName());
            emitter.onNext(1);
            emitter.onComplete();
        })
                .subscribeOn(Schedulers.io())
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
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
        assertTrue(subscribeThread.get().startsWith("rx-io-"));
    }

    @Test
    void observeOnDeliversEventsOnComputationScheduler() throws Exception {
        AtomicReference<String> observeThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
        })
                .observeOn(Schedulers.computation())
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        observeThread.set(Thread.currentThread().getName());
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
        assertTrue(observeThread.get().startsWith("rx-computation-"));
    }

    @Test
    void singleSchedulerUsesOneThread() throws Exception {
        List<String> threads = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(2);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        })
                .observeOn(Schedulers.single())
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer item) {
                        threads.add(Thread.currentThread().getName());
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                    }
                });

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertFalse(threads.isEmpty());
        assertEquals(1, threads.stream().distinct().count());
        assertTrue(threads.get(0).startsWith("rx-single-"));
    }
}
