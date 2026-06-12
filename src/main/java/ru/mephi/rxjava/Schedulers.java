package ru.mephi.rxjava;

import ru.mephi.rxjava.schedulers.ComputationScheduler;
import ru.mephi.rxjava.schedulers.IOThreadScheduler;
import ru.mephi.rxjava.schedulers.SingleThreadScheduler;

/**
 * Фабрика планировщиков, аналогичная Schedulers из RxJava.
 */
public final class Schedulers {

    private static final Scheduler IO = new IOThreadScheduler();
    private static final Scheduler COMPUTATION = new ComputationScheduler();
    private static final Scheduler SINGLE = new SingleThreadScheduler();

    private Schedulers() {
    }

    public static Scheduler io() {
        return IO;
    }

    public static Scheduler computation() {
        return COMPUTATION;
    }

    public static Scheduler single() {
        return SINGLE;
    }
}
