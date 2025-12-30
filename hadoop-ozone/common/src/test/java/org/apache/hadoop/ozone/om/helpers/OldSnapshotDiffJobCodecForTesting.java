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

package org.apache.hadoop.ozone.om.helpers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.hadoop.hdds.utils.db.Codec;

/**
 * Codec to serialize / deserialize SnapshotDiffJob.
 */
public class OldSnapshotDiffJobCodecForTesting
    implements Codec<SnapshotDiffJob> {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .setSerializationInclusion(JsonInclude.Include.NON_NULL)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @Override
  public Class<SnapshotDiffJob> getTypeClass() {
    return SnapshotDiffJob.class;
  }

  @Override
  public byte[] toPersistedFormatImpl(SnapshotDiffJob object) throws IOException {
    return MAPPER.writeValueAsBytes(object);
  }

  @Override
  public SnapshotDiffJob fromPersistedFormatImpl(byte[] rawData) throws IOException {
    return MAPPER.readValue(rawData, SnapshotDiffJob.class);
  }

  @Override
  public SnapshotDiffJob copyObject(SnapshotDiffJob object) {
    // Note: Not really a "copy". from OmDBDiffReportEntryCodec
    return object;
  }
}
