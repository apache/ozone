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

package org.apache.hadoop.ozone.container.common.states.endpoint;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_ADDRESS_REFRESH_MISSED_COUNT_THRESHOLD;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.net.HostAndPort;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.protocolPB.StorageContainerDatanodeProtocolClientSideTranslatorPB;
import org.junit.jupiter.api.Test;

/**
 * Verifies a connection-class heartbeat failure past the threshold triggers a DNS re-resolution of
 * the SCM peer (HDDS-15533); flag-off, application errors, and below-threshold do not.
 */
public class TestHeartbeatEndpointTaskDnsRefresh {

  private static final HostAndPort SCM = new HostAndPort("test-scm-1", 9861);

  @Test
  public void connectionFailureAtThresholdTriggersRefresh() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    conf.setInt(HDDS_HEARTBEAT_ADDRESS_REFRESH_MISSED_COUNT_THRESHOLD, 2);
    SCMConnectionManager cm = runHeartbeat(conf, 3, new ConnectException("refused"));
    verify(cm, times(1)).refreshSCMServer(eq(SCM), any());
  }

  @Test
  public void flagOffSuppressesRefresh() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, false);
    SCMConnectionManager cm = runHeartbeat(conf, 5, new ConnectException("refused"));
    verify(cm, never()).refreshSCMServer(any(), any());
  }

  @Test
  public void applicationErrorDoesNotTriggerRefresh() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    SCMConnectionManager cm = runHeartbeat(conf, 5, new IOException("application-level"));
    verify(cm, never()).refreshSCMServer(any(), any());
  }

  @Test
  public void belowThresholdDoesNotTriggerRefresh() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_CLIENT_FAILOVER_RESOLVE_NEEDED_KEY, true);
    conf.setInt(HDDS_HEARTBEAT_ADDRESS_REFRESH_MISSED_COUNT_THRESHOLD, 5);
    SCMConnectionManager cm = runHeartbeat(conf, 1, new ConnectException("refused"));
    verify(cm, never()).refreshSCMServer(any(), any());
  }

  /**
   * Drives one heartbeat that fails with {@code failure}, with the endpoint reporting
   * {@code missedCount} missed heartbeats, and returns the mocked connection manager to verify.
   */
  private SCMConnectionManager runHeartbeat(OzoneConfiguration conf, long missedCount,
      IOException failure) throws Exception {
    StorageContainerDatanodeProtocolClientSideTranslatorPB proxy =
        mock(StorageContainerDatanodeProtocolClientSideTranslatorPB.class);
    when(proxy.sendHeartbeat(any())).thenThrow(failure);

    EndpointStateMachine endpoint = mock(EndpointStateMachine.class);
    when(endpoint.getEndPoint()).thenReturn(proxy);
    when(endpoint.getAddress()).thenReturn(SCM);
    when(endpoint.getMissedCount()).thenReturn(missedCount);
    when(endpoint.isPassive()).thenReturn(false);

    SCMConnectionManager connectionManager = mock(SCMConnectionManager.class);
    DatanodeStateMachine dsm = mock(DatanodeStateMachine.class);
    when(dsm.getConnectionManager()).thenReturn(connectionManager);
    when(dsm.getQueuedCommandCount())
        .thenReturn(new EnumCounters<>(SCMCommandProto.Type.class));
    StateContext context = new StateContext(conf, DatanodeStates.RUNNING, dsm, "");

    HDDSLayoutVersionManager lvm = mock(HDDSLayoutVersionManager.class);
    when(lvm.getSoftwareLayoutVersion()).thenReturn(maxLayoutVersion());
    when(lvm.getMetadataLayoutVersion()).thenReturn(maxLayoutVersion());

    DatanodeDetails dn = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress("127.0.0.1")
        .build();

    HeartbeatEndpointTask.newBuilder()
        .setConfig(conf)
        .setDatanodeDetails(dn)
        .setContext(context)
        .setLayoutVersionManager(lvm)
        .setEndpointStateMachine(endpoint)
        .build()
        .call();
    return connectionManager;
  }
}
