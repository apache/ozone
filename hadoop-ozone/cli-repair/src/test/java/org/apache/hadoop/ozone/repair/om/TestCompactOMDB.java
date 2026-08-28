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

package org.apache.hadoop.ozone.repair.om;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import picocli.CommandLine;

/**
 * Tests CompactOMDB.
 */
public class TestCompactOMDB {

  private static final String COLUMN_FAMILY = "fileTable";
  private static final String RPC_ADDRESS = "om-host:9862";

  private MockedStatic<OMNodeDetails> mockedNodeDetails;
  private MockedStatic<OMAdminProtocolClientSideImpl> mockedClient;
  private OMAdminProtocolClientSideImpl omAdminClient;

  private GenericTestUtils.PrintStreamCapturer out;
  private GenericTestUtils.PrintStreamCapturer err;

  @BeforeEach
  public void setup() throws Exception {
    out = GenericTestUtils.captureOut();
    err = GenericTestUtils.captureErr();

    omAdminClient = mock(OMAdminProtocolClientSideImpl.class);

    mockedNodeDetails = mockStatic(OMNodeDetails.class);
    mockedClient = mockStatic(OMAdminProtocolClientSideImpl.class);
    mockedClient.when(() -> OMAdminProtocolClientSideImpl.createProxyForSingleOM(any(), any(), any()))
        .thenReturn(omAdminClient);
  }

  @AfterEach
  public void tearDown() {
    IOUtils.closeQuietly(out, err, mockedNodeDetails, mockedClient);
  }

  private void mockOMNodeDetails(OMNodeDetails omNodeDetails) {
    mockedNodeDetails.when(() -> OMNodeDetails.getOMNodeDetailsFromConf(any(), any(), any()))
        .thenReturn(omNodeDetails);
  }

  private int compact(String... extraArgs) {
    String[] args = new String[] {"om", "compact", "--column-family", COLUMN_FAMILY};
    String[] allArgs = new String[args.length + extraArgs.length];
    System.arraycopy(args, 0, allArgs, 0, args.length);
    System.arraycopy(extraArgs, 0, allArgs, args.length, extraArgs.length);
    CommandLine cli = new OzoneRepair().getCmd();
    return cli.execute(allArgs);
  }

  @Test
  public void testCompactWithoutNodeIdShowsResolvedAddress() throws Exception {
    OMNodeDetails omNodeDetails = mock(OMNodeDetails.class);
    when(omNodeDetails.getRpcAddressString()).thenReturn(RPC_ADDRESS);
    mockOMNodeDetails(omNodeDetails);

    compact();

    verify(omAdminClient).compactOMDB(eq(COLUMN_FAMILY), anyInt());
    assertThat(out.getOutput())
        .contains("om node: " + RPC_ADDRESS)
        .doesNotContain("om node: null");
  }

  @Test
  public void testCompactWithNodeIdShowsNodeId() throws Exception {
    OMNodeDetails omNodeDetails = mock(OMNodeDetails.class);
    mockOMNodeDetails(omNodeDetails);

    compact("--node-id", "om1");

    verify(omAdminClient).compactOMDB(eq(COLUMN_FAMILY), anyInt());
    assertThat(out.getOutput()).contains("om node: om1");
  }

  @Test
  public void testCompactWhenOMNodeDetailsNotFound() {
    mockOMNodeDetails(null);

    compact();

    assertThat(err.getOutput()).contains("Couldn't determine OM node");
  }

  @Test
  public void testCompactHAWithoutNodeIdFailsFast() throws Exception {
    CommandLine cli = new OzoneRepair().getCmd();
    cli.execute("-D", OZONE_OM_SERVICE_IDS_KEY + "=omservice",
        "om", "compact", "--column-family", COLUMN_FAMILY);

    verify(omAdminClient, never()).compactOMDB(any(), anyInt());
    assertThat(err.getOutput()).contains("specify --node-id");
  }
}
