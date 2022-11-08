package org.apache.hadoop.hdds.utils;

import java.util.Objects;
import java.util.function.Function;

/**
 * Defines a functional interface having three inputs and returns boolean as
 * output.
 */
@FunctionalInterface
public interface BooleanTriFunction<T, U, V, R> {

  R apply(T t, U u, V v);

  default <K> BooleanTriFunction<T, U, V, K> andThen(
      Function<? super R, ? extends K> after) {
    Objects.requireNonNull(after);
    return (T t, U u, V v) -> after.apply(apply(t, u, v));
  }
}
