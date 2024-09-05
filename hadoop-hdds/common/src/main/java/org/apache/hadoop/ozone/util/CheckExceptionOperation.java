package org.apache.hadoop.ozone.util;

public interface CheckExceptionOperation<T, R, E extends Exception> {
  R apply(T t) throws E;

  default <V> CheckExceptionOperation<T, V, E> andThen(CheckExceptionOperation<R, V, E> operation) throws E {
    return (T t) -> operation.apply(this.apply(t));
  }
}
