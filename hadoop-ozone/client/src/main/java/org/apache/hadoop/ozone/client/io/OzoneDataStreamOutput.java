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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.hdds.scm.storage.FileRegionStreamOutput;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * OzoneDataStreamOutput is used to write data into Ozone.
 * It uses SCM's {@link KeyDataStreamOutput} for writing the data.
 */
public class OzoneDataStreamOutput
    implements ByteBufferStreamOutput, FileRegionStreamOutput {

  private final KeyDataStreamOutput keyDataStreamOutput;

  /**
   * Constructs OzoneDataStreamOutput with KeyDataStreamOutput.
   *
   * @param keyDataStreamOutput the underlying ByteBufferStreamOutput
   */
  public OzoneDataStreamOutput(KeyDataStreamOutput keyDataStreamOutput) {
    this.keyDataStreamOutput = keyDataStreamOutput;
  }

  @Override
  public void write(ByteBuffer b, int off, int len) throws IOException {
    keyDataStreamOutput.write(b, off, len);
  }

  @Override
  public void write(File f, long off, long len) throws IOException {
    keyDataStreamOutput.write(f, off, len);
  }

  @Override
  public synchronized void flush() throws IOException {
    keyDataStreamOutput.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    //commitKey can be done here, if needed.
    keyDataStreamOutput.close();
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return keyDataStreamOutput.getCommitUploadPartInfo();
  }

  public ByteBufferStreamOutput getByteBufStreamOutput() {
    return keyDataStreamOutput;
  }
}
