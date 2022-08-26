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

import org.rocksdb.Slice;

/**
 * Managed Slice.
 */
public class ManagedSlice extends Slice {

  public ManagedSlice(byte[] var1) {
    super(var1);
  }

  @Override
  protected void finalize() throws Throwable {
    ManagedRocksObjectMetrics.INSTANCE.increaseManagedObject();
    if (this.isOwningHandle()) {
      ManagedRocksObjectMetrics.INSTANCE.increaseLeakObject();
      ManagedRocksObjectUtils.LOG.warn("{} is not closed properly",
          this.getClass().getSimpleName());
    }
    super.finalize();
  }
}
