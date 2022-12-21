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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPLICATION_COMPRESSION;

/**
 * Defines compression algorithm for container replication.
 */
public enum CopyContainerCompression {

  NO_COMPRESSION,
  GZIP,
  LZ4,
  SNAPPY,
  ZSTD;

  private static final Logger LOG =
      LoggerFactory.getLogger(CopyContainerCompression.class);

  private static final CopyContainerCompression DEFAULT_COMPRESSION =
      CopyContainerCompression.NO_COMPRESSION;
  private static final Map<CopyContainerCompression, String>
      COMPRESSION_MAPPING = ImmutableMap.copyOf(getMapping());

  private static Map<CopyContainerCompression, String> getMapping() {
    return new HashMap<CopyContainerCompression, String>() { {
        put(NO_COMPRESSION, "no_compression");
        put(GZIP, CompressorStreamFactory.GZIP);
        put(LZ4, CompressorStreamFactory.LZ4_FRAMED);
        put(SNAPPY, CompressorStreamFactory.SNAPPY_FRAMED);
        put(ZSTD, CompressorStreamFactory.ZSTANDARD);
      }};
  }

  public static Map<CopyContainerCompression, String> getCompressionMapping() {
    return COMPRESSION_MAPPING;
  }

  public static CopyContainerCompression getConf(ConfigurationSource conf) {
    try {
      return conf.getEnum(HDDS_CONTAINER_REPLICATION_COMPRESSION,
          DEFAULT_COMPRESSION);
    } catch (IllegalArgumentException e) {
      LOG.warn("Unsupported compression codec. Skip compression.");
      return DEFAULT_COMPRESSION;
    }
  }

  public static CopyContainerCompression getDefaultCompression() {
    return NO_COMPRESSION;
  }
}
