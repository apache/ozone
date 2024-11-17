package org.apache.hadoop.ozone.container.common.utils;

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
import org.apache.hadoop.ozone.container.metadata.AbstractStore;

import java.io.Closeable;

/**
 * Class enclosing a reference counted handle to DBStore.
 */
public class ReferenceCountedHandle<STORE extends AbstractStore> implements Closeable {
  private final BaseReferenceCountedDB<STORE> dbHandle;
  private volatile boolean isClosed;

  //Provide a handle with an already incremented reference.
  public ReferenceCountedHandle(BaseReferenceCountedDB<STORE> dbHandle) {
    this.dbHandle = dbHandle;
    this.isClosed = false;
  }

  public STORE getStore() {
    return dbHandle.getStore();
  }

  @Override
  public void close() {
    if (!isClosed) {
      synchronized (this) {
        if (!isClosed) {
          if (!dbHandle.isClosed()) {
            dbHandle.decrementReference();
            dbHandle.cleanup();
          }
          this.isClosed = true;
        }
      }
    }
  }
}
