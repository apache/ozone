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

package org.apache.hadoop.ozone.scm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.admin.scm.DecommissionScmSubcommand;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Unit tests to validate the TestScmDecommissionSubCommand class includes the
 * correct output when executed against a mock client.
 */
public class TestDecommissionScmSubcommand {

  @Test
  public void testDecommissionScmInputParams() throws Exception {
    // requires String <nodeId>
    DecommissionScmSubcommand cmd = new DecommissionScmSubcommand();
    ScmClient client = mock(ScmClient.class);
    OzoneAdmin admin = new OzoneAdmin();

    try (GenericTestUtils.SystemErrCapturer capture =
             new GenericTestUtils.SystemErrCapturer()) {
      String[] args = {"scm", "decommission"};
      admin.execute(args);
      assertThat(capture.getOutput()).contains(
          "Usage: ozone admin scm decommission");
    }

    // now give required String <nodeId>
    CommandLine c1 = new CommandLine(cmd);
    String scmId = UUID.randomUUID().toString();
    c1.parseArgs("--nodeid=" + scmId);

    DecommissionScmResponseProto response =
        DecommissionScmResponseProto.newBuilder()
            .setSuccess(true)
            .build();

    when(client.decommissionScm(any()))
        .thenAnswer(invocation -> (
            response));

    try (GenericTestUtils.SystemOutCapturer capture =
             new GenericTestUtils.SystemOutCapturer()) {
      cmd.execute(client);
      assertThat(capture.getOutput()).contains(scmId);
    }
  }

  @Test
  public void testDecommissionScmScmRemoveErrors() throws Exception {
    // requires String <nodeId>
    DecommissionScmSubcommand cmd = new DecommissionScmSubcommand();
    ScmClient client = mock(ScmClient.class);

    CommandLine c1 = new CommandLine(cmd);
    String scmId = UUID.randomUUID().toString();
    c1.parseArgs("--nodeid=" + scmId);

    DecommissionScmResponseProto response =
        DecommissionScmResponseProto.newBuilder()
            .setSuccess(false)
            .setErrorMsg("Cannot remove current leader.")
            .build();

    when(client.decommissionScm(any()))
        .thenAnswer(invocation -> (
            response));

    try (GenericTestUtils.SystemOutCapturer capture = new GenericTestUtils.SystemOutCapturer()) {
      IOException ioe = assertThrows(IOException.class, () -> cmd.execute(client));
      assertThat(ioe.getMessage()).contains("remove current leader");
    }
  }
}
