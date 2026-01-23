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
import java.io.OutputStream;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.KeyMetadataAware;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;

/**
 * OzoneOutputStream stub for testing.
 */
public class OzoneOutputStreamStub extends OzoneOutputStream {

  private final String partName;
  private boolean closed;

  /**
   * Constructs OzoneOutputStreamStub with outputStream and partName.
   *
   * @param outputStream
   * @param name - partName
   */
  public OzoneOutputStreamStub(OutputStream outputStream, String name) {
    super(outputStream, null);
    this.partName = name;
  }

  @Override
  public void write(int b) throws IOException {
    getOutputStream().write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    getOutputStream().write(b, off, len);
  }

  @Override
  public synchronized void flush() throws IOException {
    getOutputStream().flush();
  }

  @Override
  public synchronized void close() throws IOException {
    //commitKey can be done here, if needed.
    if (!closed) {
      getOutputStream().close();
      closed = true;
    }
  }

  @Override
  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    return closed ? new OmMultipartCommitUploadPartInfo(partName,
        ((KeyMetadataAware)getOutputStream()).getMetadata().get(OzoneConsts.ETAG)) : null;
  }
}
