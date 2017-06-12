package io.github.retz.misc;

public interface Receivable<T, E extends Throwable> {
    void receive(T value) throws E;
}
