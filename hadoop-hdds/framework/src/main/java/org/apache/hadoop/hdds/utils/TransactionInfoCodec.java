/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.utils;

import org.apache.hadoop.hdds.utils.db.Codec;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Codec to convert {@link TransactionInfo} to byte array and from byte array
 * to {@link TransactionInfo}.
 */
public class TransactionInfoCodec implements Codec<TransactionInfo> {
  @Override
  public byte[] toPersistedFormat(TransactionInfo object) throws IOException {
    checkNotNull(object, "Null object can't be converted to byte array.");
    return object.convertToByteArray();
  }

  @Override
  public TransactionInfo fromPersistedFormat(byte[] rawData)
      throws IOException {
    checkNotNull(rawData, "Null byte array can't be converted to " +
        "real object.");
    return TransactionInfo.getFromByteArray(rawData);
  }

  @Override
  public TransactionInfo copyObject(TransactionInfo object) {
    return object;
  }
}
