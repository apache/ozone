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

package org.apache.hadoop.ozone.debug.replicas;

import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
 * Class to hold the verification results for a key.
 */
public class KeyVerificationResult {
  private final boolean keyPass;
  private final String volumeName;
  private final String bucketName;
  private final String keyName;
  private final List<BlockVerificationData> blockResults;

  public KeyVerificationResult(String volumeName, String bucketName, String keyName,
      List<BlockVerificationData> blockResults, boolean keyPass) {
    this.volumeName = volumeName;
    this.bucketName = bucketName;
    this.keyName = keyName;
    this.blockResults = blockResults;
    this.keyPass = keyPass;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getKeyName() {
    return keyName;
  }

  public List<BlockVerificationData> getBlockResults() {
    return blockResults;
  }

  public boolean isKeyPass() {
    return keyPass;
  }

  /**
   * Class to hold the verification results for a block.
   */
  public static class BlockVerificationData {
    private final long containerID;
    private final long blockID;
    private final List<ReplicaVerificationData> replicaResults;
    private final boolean blockPass;

    public BlockVerificationData(long containerID, long blockID,
        List<ReplicaVerificationData> replicaResults,
        boolean blockPass) {
      this.containerID = containerID;
      this.blockID = blockID;
      this.replicaResults = replicaResults;
      this.blockPass = blockPass;
    }

    public long getContainerID() {
      return containerID;
    }

    public long getBlockID() {
      return blockID;
    }

    public List<ReplicaVerificationData> getReplicaResults() {
      return replicaResults;
    }

    public boolean isBlockPass() {
      return blockPass;
    }
  }

  /**
   * Class to hold the verification results for a replica.
   */
  public static class ReplicaVerificationData {
    private final DatanodeDetails datanode;
    private final int replicaIndex;
    private final List<CheckData> checkResults;
    private final boolean replicaPass;

    public ReplicaVerificationData(DatanodeDetails datanode, int replicaIndex,
        List<CheckData> checkResults,
        boolean replicaPass) {
      this.datanode = datanode;
      this.replicaIndex = replicaIndex;
      this.checkResults = checkResults;
      this.replicaPass = replicaPass;
    }

    public DatanodeDetails getDatanode() {
      return datanode;
    }

    public int getReplicaIndex() {
      return replicaIndex;
    }

    public List<CheckData> getCheckResults() {
      return checkResults;
    }

    public boolean isReplicaPass() {
      return replicaPass;
    }
  }

  /**
   * Class to hold the results of a single check.
   */
  public static class CheckData {
    private final String type;
    private final boolean completed;
    private final boolean pass;
    private final List<String> failures;

    public CheckData(String type, boolean completed, boolean pass,
        List<String> failures) {
      this.type = type;
      this.completed = completed;
      this.pass = pass;
      this.failures = failures;
    }

    public String getType() {
      return type;
    }

    public boolean isCompleted() {
      return completed;
    }

    public boolean isPass() {
      return pass;
    }

    public List<String> getFailures() {
      return failures;
    }
  }
}
