/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils;

import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collection;

/**
 * Static helper utilities for IO / Closable classes.
 */
public final class IOUtils {

  private IOUtils() {
  }

  /**
   * Close the Closeable objects and <b>ignore</b> any {@link Throwable} or
   * null pointers. Must only be used for cleanup in exception handlers.
   *
   * @param logger     the log to record problems to at debug level. Can be
   *                   null.
   * @param closeables the objects to close
   */
  public static void cleanupWithLogger(Logger logger, AutoCloseable... closeables) {
    if (closeables == null) {
      return;
    }
    for (AutoCloseable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch (Throwable e) {
          if (logger != null) {
            logger.debug("Exception in closing {}", c, e);
          }
        }
      }
    }
  }

  /**
   * Close each argument, catching exceptions and logging them as error.
   */
  public static void close(Logger logger, AutoCloseable... closeables) {
    close(logger, Arrays.asList(closeables));
  }

  /**
   * Close each argument, catching exceptions and logging them as error.
   */
  public static void close(Logger logger,
      Collection<? extends AutoCloseable> closeables) {
    if (closeables == null) {
      return;
    }
    for (AutoCloseable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch (Exception e) {
          if (logger != null) {
            logger.error("Exception in closing {}", c, e);
          }
        }
      }
    }
  }

  /**
   * Close each argument, swallowing exceptions.
   */
  public static void closeQuietly(AutoCloseable... closeables) {
    close(null, closeables);
  }

  /**
   * Close each argument, swallowing exceptions.
   */
  public static void closeQuietly(Collection<? extends AutoCloseable> closeables) {
    close(null, closeables);
  }
}
