package org.apache.hadoop.hdds.utils;

/**
 * Defines a functional interface to call void returning function.
 */
@FunctionalInterface
public interface VoidCallable<EXCEPTION_TYPE extends Exception> {
    void call() throws EXCEPTION_TYPE;
  }