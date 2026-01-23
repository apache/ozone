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

package org.apache.hadoop.ozone.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.KeyDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;

/**
 * OzoneDataStreamOutput stub for testing.
 */
public class OzoneDataStreamOutputStub extends OzoneDataStreamOutput {

  private final String partName;
  private boolean closed = false;

  /**
   * Constructs OzoneDataStreamOutputStub with streamOutput and partName.
   */
  public OzoneDataStreamOutputStub(
      ByteBufferStreamOutput byteBufferStreamOutput,
      String partName) {
    super(byteBufferStreamOutput, byteBufferStreamOutput);
    this.partName = partName;
  }

  @Override
  public void write(ByteBuffer b, int off, int len) throws IOException {
    getByteBufStreamOutput().write(b, off, len);
  }

  @Override
  public synchronized void flush() throws IOException {
    getByteBufStreamOutput().flush();
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      getByteBufStreamOutput().close();
      closed = true;
    }
  }

  @Override
  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return closed ? new OmMultipartCommitUploadPartInfo(partName,
        getMetadata().get(OzoneConsts.ETAG)) : null;
  }

  @Override
  public KeyDataStreamOutput getKeyDataStreamOutput() {
    ByteBufferStreamOutput streamOutput = getByteBufStreamOutput();
    if (streamOutput instanceof KeyDataStreamOutput) {
      return (KeyDataStreamOutput) streamOutput;
    }
    return super.getKeyDataStreamOutput();
  }
}
