/**
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
package org.apache.hadoop.hdds.client;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.regex.Pattern;

/**
 * Validator to check if replication config is enabled.
 */
@ConfigGroup(prefix = "ozone.replication")
public class ReplicationConfigValidator {

  @Config(key = "allowed-configs",
      defaultValue = "^((STANDALONE|RATIS)/(ONE|THREE))|(EC/(3-2|6-3|10-4))$",
      type = ConfigType.STRING,
      description = "Regular expression to restrict enabled " +
          "replication schemes",
      tags = ConfigTag.STORAGE)
  private String validationPattern;

  private Pattern validationRegexp;

  @PostConstruct
  public void init() {
    if (validationPattern != null && !validationPattern.equals("")) {
      validationRegexp = Pattern.compile(validationPattern);
    }
  }

  public ReplicationConfig validate(ReplicationConfig replicationConfig) {
    if (validationRegexp == null) {
      return replicationConfig;
    }
    if (!validationRegexp.matcher(
            replicationConfig.configFormat()).matches()) {
      String replication = replicationConfig.getReplication();
      if (HddsProtos.ReplicationType.EC ==
                replicationConfig.getReplicationType()) {
        ECReplicationConfig ecConfig =
              (ECReplicationConfig) replicationConfig;
        replication =  ecConfig.getCodec() + "-" + ecConfig.getData() +
                "-" + ecConfig.getParity() + "-{CHUNK_SIZE}";
        //EC type checks data-parity
        throw new IllegalArgumentException(
                "Invalid data-parity replication config " +
                        "for type " + replicationConfig.getReplicationType() +
                        " and replication " + replication + "." +
                        " Supported data-parity are 3-2,6-3,10-4");
      }
      //Non-EC type
      throw new IllegalArgumentException("Invalid replication config " +
              "for type " + replicationConfig.getReplicationType() +
              " and replication " + replication);
    }
    return replicationConfig;
  }

}
