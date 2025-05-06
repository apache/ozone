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

package org.apache.hadoop.ozone.freon;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeIdYaml;
import org.apache.ratis.util.Preconditions;
import picocli.CommandLine.Option;

/**
 * Generic utility for leader/follower specific isolated tests.
 */
public class BaseAppendLogGenerator extends BaseFreonGenerator {

  @SuppressWarnings("checkstyle:VisibilityModifier")
  @Option(names = {"-r", "--raft-peer"},
      description = "Set the UUID of the raft peer managed by the datanode.",
      defaultValue = "")
  protected String serverId;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  @Option(names = {"-c", "--server-address"},
      description = "Host:port of the Ratis server",
      defaultValue = "localhost:9858")
  protected String serverAddress = "localhost:9858";

  @SuppressWarnings("checkstyle:VisibilityModifier")
  @Option(names = {"--inflight-limit"},
      description = "Maximum in-flight messages",
      defaultValue = "10")
  protected int inflightLimit;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected BlockingQueue<Long> inFlightMessages;

  protected void setServerIdFromFile(OzoneConfiguration conf) throws
      IOException {
    File idFile = new File(HddsServerUtil.getDatanodeIdFilePath(conf));
    if ((this.serverId == null || this.serverId.equals("")) &&
        idFile.exists()) {
      DatanodeDetails datanodeDetails =
          DatanodeIdYaml.readDatanodeIdFile(idFile);
      this.serverId = datanodeDetails.getUuidString();
    }
    Preconditions.assertTrue(!serverId.equals(""),
        "Server id is not specified and can't be read from " + idFile
            .getAbsolutePath());
  }
}
