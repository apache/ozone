package org.apache.hadoop.ozone.util;

/**
 *
 * Represents a function that accepts one argument and produces a result.
 * This is a functional interface whose functional method is apply(Object).
 * Type parameters:
 * <T> – the type of the input to the function <R> – the type of the result of the function
 * <E> - the type of exception thrown.
 */
public interface CheckedExceptionOperation<T, R, E extends Exception> {
  R apply(T t) throws E;

  default <V> CheckedExceptionOperation<T, V, E> andThen(CheckedExceptionOperation<R, V, E> operation) throws E {
    return (T t) -> operation.apply(this.apply(t));
  }
}
