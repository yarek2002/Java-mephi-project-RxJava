package ru.mephi.rxjava.schedulers;

import ru.mephi.rxjava.Scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scheduler для IO-операций на базе CachedThreadPool.
 */
public final class IOThreadScheduler implements Scheduler {

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(new NamedFactory("rx-io"));

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
