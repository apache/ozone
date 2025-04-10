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
import java.util.Collections;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

/**
 * Class that downloads every replica for all the blocks associated with a
 * given key. It also generates a manifest file with information about the
 * downloaded replicas.
 */
public class Checksums implements ReplicaVerifier {
  private static final String CHECKTYPE = "checksum";

  public Checksums() {
  }

  @Override
  public BlockVerificationResult verifyBlock(DatanodeDetails datanode, OzoneInputStream stream,
        OmKeyLocationInfo keyLocation) {
    try (InputStream is = stream) {
      IOUtils.copyLarge(is, NullOutputStream.INSTANCE);

      return new BlockVerificationResult(CHECKTYPE, true, Collections.emptyList());
    } catch (IOException e) {
      BlockVerificationResult.FailureDetail failure = new BlockVerificationResult
          .FailureDetail(true, e.getMessage());
      return new BlockVerificationResult(CHECKTYPE, false, Collections.singletonList(failure));
    }
  }

}
