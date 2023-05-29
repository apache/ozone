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
 */
package org.apache.hadoop.ozone.container.metadata;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;

/**
 * Codec for parsing {@link ChunkInfoList} objects from data
 * that may have been written using schema version one. Before upgrading
 * schema versions, deleted block IDs were stored with a duplicate copy of
 * their ID as the value in the database. After upgrading the code, any
 * deletes that happen on the DB will save the chunk information with the
 * deleted blocks instead, even if those deletes are performed on a database
 * created with schema version one.
 * <p>
 * Since the resulting database may contain a mix of values that could either
 * be block IDs or chunk information, this codec will attempt to parse bytes as
 * chunk information, and on failure, throw an exception with a message about
 * the old schema format.
 * <p>
 * Note that there is currently no where in the code (except for tests) where
 * the values associated with deleted block IDs are used. If it is added in the
 * future, callers should be aware that the associated chunk information may not
 * always be present.
 */
public final class SchemaOneChunkInfoListCodec {
  /** @return {@link ChunkInfoList#getCodec()}. */
  public static Codec<ChunkInfoList> get() {
    return ChunkInfoList.getCodec();
  }

  private SchemaOneChunkInfoListCodec() {
    // singleton
  }
}
