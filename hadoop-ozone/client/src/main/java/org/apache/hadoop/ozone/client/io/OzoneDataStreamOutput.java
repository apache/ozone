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

import io.netty.buffer.ByteBuf;
import org.apache.hadoop.hdds.scm.storage.ByteBufStreamOutput;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;

import java.io.IOException;

/**
 * OzoneDataStreamOutput is used to write data into Ozone.
 * It uses SCM's {@link KeyDataStreamOutput} for writing the data.
 */
public class OzoneDataStreamOutput implements ByteBufStreamOutput {

  private final ByteBufStreamOutput byteBufStreamOutput;

  /**
   * Constructs OzoneDataStreamOutput with KeyDataStreamOutput.
   *
   * @param byteBufStreamOutput
   */
  public OzoneDataStreamOutput(ByteBufStreamOutput byteBufStreamOutput) {
    this.byteBufStreamOutput = byteBufStreamOutput;
  }

  @Override
  public void write(ByteBuf b) throws IOException {
    byteBufStreamOutput.write(b);
  }

  @Override
  public synchronized void flush() throws IOException {
    byteBufStreamOutput.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    //commitKey can be done here, if needed.
    byteBufStreamOutput.close();
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    if (byteBufStreamOutput instanceof KeyDataStreamOutput) {
      return ((KeyDataStreamOutput)
              byteBufStreamOutput).getCommitUploadPartInfo();
    }
    // Otherwise return null.
    return null;
  }

  public ByteBufStreamOutput getByteBufStreamOutput() {
    return byteBufStreamOutput;
  }
}
