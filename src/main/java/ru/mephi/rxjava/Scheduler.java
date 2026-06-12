package ru.mephi.rxjava;

/**
 * Планировщик выполнения задач.
 */
public interface Scheduler {

    void execute(Runnable task);
}
