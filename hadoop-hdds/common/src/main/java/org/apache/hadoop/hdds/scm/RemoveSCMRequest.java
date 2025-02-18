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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

/**
 * Request class using which SCM can be removed form the HA Ring.
 */
public class RemoveSCMRequest {

  private final String clusterId;
  private final String scmId;
  private final String ratisAddr;

  public RemoveSCMRequest(String clusterId, String scmId, String addr) {
    this.clusterId = clusterId;
    this.scmId = scmId;
    this.ratisAddr = addr;
  }

  public HddsProtos.RemoveScmRequestProto getProtobuf() {
    return HddsProtos.RemoveScmRequestProto.newBuilder().setClusterId(clusterId)
        .setScmId(scmId).setRatisAddr(ratisAddr).build();
  }

  /**
   * Gets the clusterId from the Version file.
   * @return ClusterId
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * Gets the SCM Id from the Version file.
   * @return SCM Id
   */
  public String getScmId() {
    return scmId;
  }

  public String getRatisAddr() {
    return ratisAddr;
  }

}
