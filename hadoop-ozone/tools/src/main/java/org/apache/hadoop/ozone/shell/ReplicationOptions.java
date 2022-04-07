/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.shell;

import org.apache.hadoop.fs.ozone.OzoneClientUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import picocli.CommandLine;

import java.util.Optional;

/**
 * Options for specifying replication config.
 */
public class ReplicationOptions {

  @CommandLine.Option(names = {"--replication", "-r"},
      description = "Replication definition. Valid values are replication"
          + " type-specific.  For RATIS: ONE or THREE."
          + " In case of EC, pass CODEC-DATA-PARITY-CHUNKSIZE, "
          + " e.g. rs-3-2-1024k, rs-6-3-1024k, rs-10-4-1024k")
  private String replication;

  @CommandLine.Option(names = {"--type", "--replication-type"},
      description = "Replication type. Supported types are: RATIS, EC")
  private ReplicationType replicationType;

  private final ConfigurationSource conf = new OzoneConfiguration();

  public Optional<ReplicationConfig> replicationConfig() {
    return Optional.ofNullable(
        OzoneClientUtils.validateAndGetClientReplicationConfig(
            replicationType, replication, conf));
  }

  public void setReplication(String replication) {
    this.replication = replication;
  }

  public void setReplicationType(ReplicationType replicationType) {
    this.replicationType = replicationType;
  }
}
