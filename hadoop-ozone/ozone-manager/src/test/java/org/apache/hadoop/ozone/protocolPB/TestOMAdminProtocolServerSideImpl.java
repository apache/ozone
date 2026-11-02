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

package org.apache.hadoop.ozone.protocolPB;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.GetPeerUpgradeStatusRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerAdminProtocolProtos.GetPeerUpgradeStatusResponse;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OMAdminProtocolServerSideImpl}.
 */
public class TestOMAdminProtocolServerSideImpl {

  @Test
  public void testGetPeerUpgradeStatusReturnsSoftwareVersion() throws ServiceException {
    OzoneManager om = mock(OzoneManager.class);

    OMAdminProtocolServerSideImpl handler = new OMAdminProtocolServerSideImpl(om);
    GetPeerUpgradeStatusResponse response =
        handler.getPeerUpgradeStatus(null, GetPeerUpgradeStatusRequest.newBuilder().build());

    assertEquals(OzoneManagerVersion.SOFTWARE_VERSION.serialize(), response.getOmSoftwareVersion());
  }
}
