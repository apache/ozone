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
package org.apache.hadoop.hdds.utils.db;

import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.rocksdb.ColumnFamilyHandle;

/**
 * Class represents a read write batch operation,
 * collects multiple db operation.
 */
public interface RWBatchOperation extends BatchOperation {
  /**
   * Creates the iterator object.
   * @param handle the handler of table column family
   * @param newIterator the iterator
   * @return ManagedRocksIterator
   * @throws IOException
   */
  ManagedRocksIterator newIteratorWithBase(
      ColumnFamilyHandle handle, ManagedRocksIterator newIterator)
      throws IOException;

  /**
   * Increase refCount for object usages.
   */
  void incrementRefCount();

  /**
   * Decrement refCount. Once refCount is 0, it will release object.
   * Ref Count increment and decrement needs managed by caller.
   */
  void decrementRefCount();
}
