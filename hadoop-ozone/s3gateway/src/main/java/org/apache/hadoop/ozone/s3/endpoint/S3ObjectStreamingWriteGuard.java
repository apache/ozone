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

package org.apache.hadoop.ozone.s3.endpoint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.hadoop.ozone.client.io.KeyMetadataAware;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;

/**
 * Guards close-time commit for write request using datastream output.
 */
final class S3ObjectStreamingWriteGuard extends S3ObjectWriteGuard {

  private final OzoneDataStreamOutput outputStream;

  S3ObjectStreamingWriteGuard(
      OzoneDataStreamOutput outputStream, long expectedLength, String keyPath) {
    super(outputStream, expectedLength, keyPath);
    this.outputStream = outputStream;
    outputStream.setPreCommits(getPreCommits());
  }

  @Override
  protected void write(byte[] buffer, int offset, int length)
      throws IOException {
    outputStream.write(ByteBuffer.wrap(buffer, offset, length));
  }

  @Override
  public Map<String, String> getMetadata() {
    return ((KeyMetadataAware) outputStream).getMetadata();
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
  }
}
