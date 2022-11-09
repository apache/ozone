/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.RocksObject;

/**
 * General template for a managed RocksObject.
 * @param <T>
 */
class ManagedObject<T extends RocksObject> implements AutoCloseable {
  private final T original;

  private final StackTraceElement[] elements;

  ManagedObject(T original) {
    this.original = original;
    if (ManagedRocksObjectUtils.LOG.isDebugEnabled()) {
      this.elements = Thread.currentThread().getStackTrace();
    } else {
      this.elements = null;
    }
  }

  public T get() {
    return original;
  }

  @Override
  public void close() {
    original.close();
  }

  @Override
  protected void finalize() throws Throwable {
    ManagedRocksObjectUtils.assertClosed(this);
    super.finalize();
  }

  public String getStackTrace() {
    if (elements != null && elements.length > 0) {
      StringBuilder sb = new StringBuilder();
      for (int line = 1; line < elements.length; line++) {
        sb.append(elements[line]);
        sb.append("\n");
      }
      return sb.toString();
    }
    return "";
  }

}
