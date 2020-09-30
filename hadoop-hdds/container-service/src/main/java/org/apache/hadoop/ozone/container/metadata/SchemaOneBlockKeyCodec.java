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
package org.apache.hadoop.ozone.container.metadata;

import java.io.IOException;

import com.google.common.primitives.Longs;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Codec to convert String to/from byte array.
 */
public class SchemaOneBlockKeyCodec implements Codec<String> {

  @Override
  public byte[] toPersistedFormat(String stringObject) {
    try {
      long longObject = Long.parseLong(stringObject);
      return Longs.toByteArray(longObject);
    } catch (NumberFormatException ex) {
      return StringUtils.string2Bytes(stringObject);
    }
  }

  @Override
  public String fromPersistedFormat(byte[] rawData) throws IOException {
    String result = StringUtils.bytes2String(rawData);

    if (!result.startsWith(OzoneConsts.DELETING_KEY_PREFIX)) {
      result = Long.toString(Longs.fromByteArray(rawData));
    }

    return result;
  }

  @Override
  public String copyObject(String object) {
    return object;
  }
}
