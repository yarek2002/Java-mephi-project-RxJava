package ru.mephi.rxjava.schedulers;

import ru.mephi.rxjava.Scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scheduler для вычислений на базе FixedThreadPool.
 */
public final class ComputationScheduler implements Scheduler {

    private static final int THREAD_COUNT = Math.max(2, Runtime.getRuntime().availableProcessors());
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(
            THREAD_COUNT,
            new NamedFactory("rx-computation")
    );

    @Override
    public void execute(Runnable task) {
        EXECUTOR.execute(task);
    }

    private static final class NamedFactory implements ThreadFactory {

        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(1);

        NamedFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, prefix + "-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}
