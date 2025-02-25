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

package org.apache.hadoop.hdds.scm.storage;

import java.util.List;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;

/**
 * This class executes watchForCommit on ratis pipeline and releases
 * buffers once data successfully gets replicated.
 */
class StreamCommitWatcher extends AbstractCommitWatcher<StreamBuffer> {
  private final List<StreamBuffer> bufferList;

  StreamCommitWatcher(XceiverClientSpi xceiverClient,
      List<StreamBuffer> bufferList) {
    super(xceiverClient);
    this.bufferList = bufferList;
  }

  @Override
  void releaseBuffers(long index) {
    long acked = 0;
    for (StreamBuffer buffer : remove(index)) {
      acked += buffer.position();
      bufferList.remove(buffer);
    }
    addAckDataLength(acked);
  }
}
