/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.client;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory to create the mock datanode clients.
 */
public class MockXceiverClientFactory
    implements XceiverClientFactory {

  private final Map<DatanodeDetails, MockDatanodeStorage> storage =
      new HashMap<>();
  private final Map<IOException, Set<DatanodeDetails>> pendingDNFailures =
      new HashMap<>();

  public void setFailedStorages(List<DatanodeDetails> failedStorages) {
    mockStorageFailure(failedStorages,
        new IOException("This storage was marked as failed."));
  }

  public void mockStorageFailure(Collection<DatanodeDetails> datanodes,
      IOException reason) {
    if (!pendingDNFailures.containsKey(reason)) {
      pendingDNFailures.put(reason, new HashSet<>());
    }
    pendingDNFailures.get(reason).addAll(datanodes);
    mockStorageFailure(reason);
  }

  private void mockStorageFailure(IOException reason) {

    Collection<DatanodeDetails> datanodes =
        pendingDNFailures.getOrDefault(reason, Collections.emptySet());

    Iterator<DatanodeDetails> iterator = datanodes.iterator();
    while (iterator.hasNext()) {
      DatanodeDetails dn = iterator.next();
      MockDatanodeStorage mockDN = storage.get(dn);
      if (mockDN != null) {
        mockDN.setStorageFailed(reason);
        iterator.remove();
      }
    }

    if (datanodes.isEmpty()) {
      pendingDNFailures.remove(reason);
    }
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public XceiverClientSpi acquireClient(Pipeline pipeline)
      throws IOException {
    MockXceiverClientSpi mockXceiverClientSpi =
        new MockXceiverClientSpi(pipeline, storage
            .computeIfAbsent(pipeline.getFirstNode(),
                r -> new MockDatanodeStorage()));
    // Incase if this node already set to mark as failed.
    for (IOException reason : pendingDNFailures.keySet()) {
      mockStorageFailure(reason);
    }
    return mockXceiverClientSpi;
  }

  @Override
  public void releaseClient(XceiverClientSpi xceiverClient,
      boolean invalidateClient) {

  }

  @Override
  public XceiverClientSpi acquireClientForReadData(Pipeline pipeline)
      throws IOException {
    return new MockXceiverClientSpi(pipeline, storage
        .computeIfAbsent(pipeline.getFirstNode(),
            r -> new MockDatanodeStorage()));
  }

  @Override
  public void releaseClientForReadData(XceiverClientSpi xceiverClient,
      boolean b) {

  }

  /**
   * Returns data nodes details.
   */
  public Map<DatanodeDetails, MockDatanodeStorage> getStorages() {
    return this.storage;
  }
}


