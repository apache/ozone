/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db.managed;

import java.nio.ByteBuffer;
import org.rocksdb.DirectSlice;

/**
 * ManagedDirectSlice is a class that extends the {@link DirectSlice} class and provides additional
 * management for slices of direct {@link ByteBuffer} memory. This class initializes the slice with
 * the given ByteBuffer and sets its prefix and length properties based on the buffer's position
 * and remaining capacity.
 *
 * The class is designed to handle specific memory slicing operations while ensuring that the
 * provided ByteBuffer’s constraints are respected. ManagedDirectSlice leverages its parent
 * {@link DirectSlice} functionalities to deliver optimized direct buffer handling.
 *
 * Constructor:
 * - Initializes the ManagedDirectSlice instance with a provided ByteBuffer.
 * - Sets the slice length to the buffer's remaining capacity.
 * - Removes the prefix based on the buffer's position.
 *
 * NOTE: This class should be only with ByteBuffer whose position and limit is going be immutable in the lifetime of
 *  this ManagedDirectSlice instance. This means that the ByteBuffer's position and limit should not be modified
 *  externally while the ManagedDirectSlice is in use. The value in the byte buffer should be only accessed via the
 *  instance.
 */
public class ManagedDirectSlice extends DirectSlice {

  public ManagedDirectSlice(ByteBuffer data) {
    super(data);
    this.removePrefix(data.position());
    this.setLength(data.remaining());
  }
}
