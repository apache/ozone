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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.ratis.util.function.CheckedRunnable;

/**
 * Tracks bytes written for a write request and guards close-time commit.
 */
class S3ObjectWriteGuard implements AutoCloseable {

  private final OutputStream outputStream;
  private final long expectedLength;
  private final List<CheckedRunnable<IOException>> preCommits =
      new ArrayList<>();
  private long writtenLength;

  S3ObjectWriteGuard(
      OzoneOutputStream outputStream, long expectedLength, String keyPath) {
    this.outputStream = outputStream;
    this.expectedLength = expectedLength;
    addContentLengthValidation(keyPath);
    outputStream.setPreCommits(getPreCommits());
  }

  protected S3ObjectWriteGuard(
      OutputStream outputStream, long expectedLength, String keyPath) {
    this.outputStream = outputStream;
    this.expectedLength = expectedLength;
    addContentLengthValidation(keyPath);
  }

  private void addContentLengthValidation(String keyPath) {
    preCommits.add(() -> EndpointBase.validateContentLength(
        expectedLength, writtenLength, keyPath).run());
  }

  protected List<CheckedRunnable<IOException>> getPreCommits() {
    return preCommits;
  }

  void addPreCommit(CheckedRunnable<IOException> preCommit) {
    preCommits.add(preCommit);
  }

  long copyFrom(InputStream body, int bufferSize) throws IOException {
    byte[] buffer = new byte[bufferSize];
    while (writtenLength < expectedLength) {
      int toRead = Math.toIntExact(
          Math.min(bufferSize, expectedLength - writtenLength));
      int readLength = body.read(buffer, 0, toRead);
      if (readLength == -1) {
        break;
      }
      write(buffer, 0, readLength);
      writtenLength += readLength;
    }
    return writtenLength;
  }

  protected void write(byte[] buffer, int offset, int length)
      throws IOException {
    outputStream.write(buffer, offset, length);
  }

  public Map<String, String> getMetadata() {
    return ((OzoneOutputStream) outputStream).getMetadata();
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
  }
}
