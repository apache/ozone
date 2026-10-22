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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODES_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODE_ID_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Collection;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.ServerNotLeaderException;
import org.apache.hadoop.hdds.scm.ha.RetriableWithFailOverException;
import org.apache.hadoop.hdds.scm.ha.RetriableWithNoFailoverException;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ipc_.RetriableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@code SCMHAUtils}.
 */
class TestSCMHAUtils {

  @Test
  void testRemoveSelfId() {
    String service = "mySCM";
    String selfId = "scm3";

    OzoneConfiguration input = new OzoneConfiguration();
    input.set(OZONE_SCM_SERVICE_IDS_KEY, service);
    input.set(OZONE_SCM_NODES_KEY + "." + service, "scm1,scm2," + selfId);
    input.set(OZONE_SCM_NODE_ID_KEY, selfId);

    OzoneConfiguration output = SCMHAUtils.removeSelfId(input, selfId);
    Collection<String> nodesWithoutSelf =
        HddsUtils.getSCMNodeIds(output, service);

    assertEquals(2, nodesWithoutSelf.size());
    assertThat(nodesWithoutSelf).doesNotContain(selfId);
  }

  @ParameterizedTest
  @ValueSource(classes = {IOException.class, IllegalArgumentException.class})
  void failsOnOtherServerException(Class<?> serverException) {
    assertEquals(RetryDecision.FAIL, getRetryDecision(fromServer(serverException)));
  }

  @ParameterizedTest
  @ValueSource(classes = {ServerNotLeaderException.class, RetriableWithFailOverException.class,
      RetriableException.class})
  void failsOverOnRetriableServerException(Class<?> serverException) {
    assertEquals(RetryDecision.FAILOVER_AND_RETRY, getRetryDecision(fromServer(serverException)));
  }

  @Test
  void retriesOnRetriableWithNoFailoverServerException() {
    assertNotEquals(RetryDecision.FAIL, getRetryDecision(fromServer(RetriableWithNoFailoverException.class)));
  }

  @Test
  void failsOverOnConnectionFailure() {
    assertEquals(RetryDecision.FAILOVER_AND_RETRY, getRetryDecision(new ConnectException("Connection refused")));
  }

  private static Exception fromServer(Class<?> serverException) {
    return new ServiceException(new RemoteException(serverException.getName(), "server error"));
  }

  private static RetryDecision getRetryDecision(Exception e) {
    return SCMHAUtils.getRetryAction(0, 0, e, 15, 2000).action;
  }
}
