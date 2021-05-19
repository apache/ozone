/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.metadata;

import org.apache.hadoop.hdds.utils.db.Table;

import java.io.IOException;

/**
 * Default implementation for DBTransactionBuffer for SCM without Ratis.
 */
public class SCMDBTransactionBufferImpl implements DBTransactionBuffer {

  public SCMDBTransactionBufferImpl() {

  }

  @Override
  public <KEY, VALUE> void addToBuffer(
      Table<KEY, VALUE> table, KEY key, VALUE value) throws IOException {
    table.put(key, value);
  }

  @Override
  public <KEY, VALUE>void removeFromBuffer(Table<KEY, VALUE> table, KEY key)
      throws IOException {
    table.delete(key);
  }

  @Override
  public void close() {
  }

}
