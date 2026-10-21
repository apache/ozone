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

package org.apache.hadoop.ozone.debug.replicas;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

/**
 * Verifies the checksum of blocks by checking each replica associated
 * with a given key.
 */
public class ChecksumVerifier implements ReplicaVerifier {
  private final OzoneConfiguration conf;
  private final ContainerOperationClient containerClient;
  private final XceiverClientManager xceiverClientManager;
  private static final String CHECK_TYPE = "checksum";

  @Override
  public String getType() {
    return CHECK_TYPE;
  }

  public ChecksumVerifier(OzoneConfiguration conf) throws IOException {
    this.conf = conf;
    this.containerClient = new ContainerOperationClient(conf);
    this.xceiverClientManager = containerClient.getXceiverClientManager();
  }

  @Override
  public BlockVerificationResult verifyBlock(DatanodeDetails datanode, OmKeyLocationInfo keyLocation) {
    Pipeline pipeline = keyLocation.getPipeline().copyForReadFromNode(datanode);

    try (InputStream is = new BlockInputStreamFactoryImpl().create(
        keyLocation.getPipeline().getReplicationConfig(),
        keyLocation,
        pipeline,
        keyLocation.getToken(),
        xceiverClientManager,
        null,
        conf.getObject(OzoneClientConfig.class))) {
      IOUtils.copyLarge(is, NullOutputStream.INSTANCE);
      return BlockVerificationResult.pass();
    } catch (IOException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      StorageContainerException storageContainerException = findStorageContainerException(e);
      if (storageContainerException != null
          && storageContainerException.getResult() == ContainerProtos.Result.NO_SUCH_BLOCK) {
        return BlockVerificationResult.failCheckAndRefreshKeyLocation(storageContainerException.getMessage());
      }
      if (cause instanceof OzoneChecksumException) {
        return BlockVerificationResult.failCheck(cause.getMessage());
      } else {
        return BlockVerificationResult.failIncomplete(cause.getMessage());
      }
    }
  }

  private static StorageContainerException findStorageContainerException(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof StorageContainerException) {
        return (StorageContainerException) current;
      }
      current = current.getCause();
    }
    return null;
  }
}
