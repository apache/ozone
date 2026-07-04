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

package org.apache.hadoop.ozone.om;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.ConnectException;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for Ozone Manager HA follower read tests.
 */
public abstract class TestOzoneManagerHAFollowerRead extends AbstractOzoneManagerHATest {

  @BeforeAll
  public static void init() throws Exception {
    initCluster(true);
  }

  protected void listVolumes(boolean checkSuccess)
      throws Exception {
    try {
      getObjectStore().getClientProxy().listVolumes(null, null, 100);
    } catch (IOException e) {
      if (!checkSuccess) {
        // If the last OM to be tried by the RetryProxy is down, we would get
        // ConnectException. Otherwise, we would get a RemoteException from the
        // last running OM as it would fail to get a quorum.
        if (e instanceof RemoteException) {
          // Linearizable read will fail with ReadIndexException if the follower does not recognize any leader
          // or leader is uncontactable. It will throw ReadException if the read submitted to Ratis encounters
          // timeout.
          assertThat(((RemoteException) e).unwrapRemoteException()).isInstanceOf(RaftException.class);
        } else if (e instanceof ConnectException) {
          assertThat(e).hasMessageContaining("Connection refused");
        } else {
          assertThat(e).hasMessageContaining("Could not determine or connect to OM Leader");
        }
      } else {
        throw e;
      }
    }
  }
}
