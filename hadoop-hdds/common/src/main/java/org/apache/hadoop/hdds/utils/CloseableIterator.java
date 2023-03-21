package org.apache.hadoop.hdds.utils;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Interface for Implementing CloseableIterators.
 *
 * @param <T> Generic Parameter for Iterating values of type 'T'
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

}
