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

package org.apache.hadoop.hdds.scm.ha;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the sequence ID types managed by {@link SequenceIdGenerator} and their persisted RocksDB keys.
 */
public enum SequenceIdType {

  LOCAL_ID("localId"),

  DEL_TXN_ID("delTxnId"),

  CONTAINER_ID("containerId"),

  /** Certificate ID for all services, including root certificates. */
  CERTIFICATE_ID("CertificateId"),

  /**
   * @deprecated Use {@link #CERTIFICATE_ID} instead.
   */
  @Deprecated
  ROOT_CERTIFICATE_ID("rootCertificateId");

  /**
   * The key string stored in the RocksDB sequenceId table.
   */
  private final String dbKey;

  /**
   * Reverse lookup map from db key string to enum constant.
   */
  private static final Map<String, SequenceIdType> DB_KEY_MAP;

  static {
    Map<String, SequenceIdType> map = new HashMap<>();
    for (SequenceIdType type : values()) {
      map.put(type.dbKey, type);
    }
    DB_KEY_MAP = Collections.unmodifiableMap(map);
  }

  SequenceIdType(String dbKey) {
    this.dbKey = dbKey;
  }

  /**
   * Returns the key string used to persist this sequence ID in RocksDB.
   * This value must not be changed to keep backward compatibility with
   * existing databases.
   */
  public String getDbKey() {
    return dbKey;
  }

  /**
   * Returns the {@link SequenceIdType} corresponding to the provided RocksDB key string, or null if unmapped.
   */
  public static SequenceIdType fromDbKey(String dbKey) {
    if (dbKey == null) {
      return null;
    }
    return DB_KEY_MAP.get(dbKey);
  }
}
