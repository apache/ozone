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

package org.apache.hadoop.hdds.utils.db;

import java.io.IOException;
import org.rocksdb.RocksDBException;

/**
 * Exceptions converted from {@link RocksDBException}.
 */
public class RocksDatabaseException extends IOException {
  private static String getStatus(RocksDBException e) {
    return e.getStatus() == null ? "NULL_STATUS" : e.getStatus().getCodeString();
  }

  private static String getMessage(String message, Exception cause) {
    return cause instanceof RocksDBException ? getStatus((RocksDBException) cause) + ": " + message : message;
  }

  /** Construct from the given cause. */
  public RocksDatabaseException(String message, Exception cause) {
    super(getMessage(message, cause), cause);
  }

  public RocksDatabaseException(String message) {
    super(message);
  }

  public RocksDatabaseException() {
    super();
  }
}
