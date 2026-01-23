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

package org.apache.hadoop.ozone.container.replication;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPLICATION_COMPRESSION;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.ConfigurationTarget;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines compression algorithm for container replication.
 */
public enum CopyContainerCompression {

  NO_COMPRESSION("no_compression") {
    @Override
    public InputStream wrap(InputStream input) {
      return input;
    }

    @Override
    public OutputStream wrap(OutputStream output) {
      return output;
    }
  },
  GZIP(CompressorStreamFactory.GZIP),
  LZ4(CompressorStreamFactory.LZ4_FRAMED),
  SNAPPY(CompressorStreamFactory.SNAPPY_FRAMED),
  ZSTD(CompressorStreamFactory.ZSTANDARD);

  private static final Logger LOG = LoggerFactory.getLogger(CopyContainerCompression.class);

  private static final CopyContainerCompression DEFAULT_COMPRESSION = CopyContainerCompression.NO_COMPRESSION;

  private final String compressorFactoryName;

  CopyContainerCompression(String compressorFactoryName) {
    this.compressorFactoryName = compressorFactoryName;
  }

  public static CopyContainerCompression getConf(ConfigurationSource conf) {
    try {
      return conf.getEnum(HDDS_CONTAINER_REPLICATION_COMPRESSION,
          DEFAULT_COMPRESSION);
    } catch (IllegalArgumentException e) {
      LOG.warn("Unsupported compression codec {}, defaulting to {}",
          conf.get(HDDS_CONTAINER_REPLICATION_COMPRESSION),
          DEFAULT_COMPRESSION);
      return DEFAULT_COMPRESSION;
    }
  }

  public void setOn(ConfigurationTarget conf) {
    conf.setEnum(HDDS_CONTAINER_REPLICATION_COMPRESSION, this);
  }

  public static CopyContainerCompression getDefaultCompression() {
    return NO_COMPRESSION;
  }

  public ContainerProtos.CopyContainerCompressProto toProto() {
    return ContainerProtos.CopyContainerCompressProto.valueOf(name());
  }

  public static CopyContainerCompression fromProto(
      ContainerProtos.CopyContainerCompressProto proto) {
    if (proto == null) {
      return getDefaultCompression();
    }

    try {
      return valueOf(proto.name());
    } catch (IllegalArgumentException e) {
      return getDefaultCompression();
    }
  }

  public InputStream wrap(InputStream input) throws IOException {
    try {
      return new CompressorStreamFactory().createCompressorInputStream(
          compressorFactoryName, input);
    } catch (CompressorException e) {
      throw toIOException(e);
    }
  }

  public OutputStream wrap(OutputStream output) throws IOException {
    try {
      return new CompressorStreamFactory().createCompressorOutputStream(
          compressorFactoryName, output);
    } catch (CompressorException e) {
      throw toIOException(e);
    }
  }

  private static IOException toIOException(CompressorException e) {
    Throwable cause = e.getCause();
    if (cause instanceof IOException) {
      return (IOException) cause;
    }
    return new IOException(e);
  }
}
