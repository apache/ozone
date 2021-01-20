/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.metadata;

import org.apache.hadoop.hdds.scm.ha.SCMTransactionInfo;
import org.apache.hadoop.hdds.utils.db.Codec;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class SCMTransactionInfoCodec implements Codec<SCMTransactionInfo> {

  @Override
  public byte[] toPersistedFormat(SCMTransactionInfo object)
      throws IOException {
    checkNotNull(object, "Null object can't be converted to byte array.");
    return object.convertToByteArray();
  }

  @Override
  public SCMTransactionInfo fromPersistedFormat(byte[] rawData)
      throws IOException {
    checkNotNull(rawData, "Null byte array can't be converted to " +
        "real object.");
    return SCMTransactionInfo.getFromByteArray(rawData);
  }

  @Override
  public SCMTransactionInfo copyObject(SCMTransactionInfo object) {
    return object;
  }
}
