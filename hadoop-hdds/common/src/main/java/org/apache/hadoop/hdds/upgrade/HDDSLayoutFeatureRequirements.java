/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.upgrade;

import com.google.common.base.Preconditions;

import java.util.Collection;

public class HDDSLayoutFeatureRequirements {
  public enum PipelineRequirements {
    CLOSE_ALL_PIPELINES,
    NONE
  }

  // No layout feature can pass SCM finalization with less than 3 datanodes
  // finalized.
  public static final int REQUIRED_MIN_FINALIZED_DATANODES = 3;

  private final int minFinalizedDatanodes;
  private final PipelineRequirements pipelineRequirements;

  private HDDSLayoutFeatureRequirements(Builder builder) {
    minFinalizedDatanodes = builder.minFinalizedDatanodes;
    pipelineRequirements = builder.pipelineRequirements;
    checkMinFinalizedDatanodes();
  }

  /**
   * Generates one requirements object by aggregating multiple requirements.
   */
  public HDDSLayoutFeatureRequirements(
      Collection<HDDSLayoutFeatureRequirements> requirements) {
    int currentMinFinalizedDatanodes = 0;
    PipelineRequirements currentPipelineRequirements =
        PipelineRequirements.NONE;

    for (HDDSLayoutFeatureRequirements req: requirements) {
      currentMinFinalizedDatanodes = Math.min(currentMinFinalizedDatanodes,
          req.minFinalizedDatanodes);

      if (req.pipelineRequirements == PipelineRequirements.CLOSE_ALL_PIPELINES) {
        currentPipelineRequirements = PipelineRequirements.CLOSE_ALL_PIPELINES;
      }
    }

    minFinalizedDatanodes = currentMinFinalizedDatanodes;
    pipelineRequirements = currentPipelineRequirements;
    checkMinFinalizedDatanodes();
  }

  public int getMinFinalizedDatanodes() {
    return minFinalizedDatanodes;
  }

  public PipelineRequirements getPipelineRequirements() {
    return pipelineRequirements;
  }

  private void checkMinFinalizedDatanodes() {
    Preconditions.checkArgument(
        minFinalizedDatanodes >= REQUIRED_MIN_FINALIZED_DATANODES);
  }

   public static final class Builder {
    private int minFinalizedDatanodes;
    private PipelineRequirements pipelineRequirements;

    public Builder() {
      // Default values.
      this.minFinalizedDatanodes = REQUIRED_MIN_FINALIZED_DATANODES;
      this.pipelineRequirements = PipelineRequirements.NONE;
    }

    public Builder setMinFinalizedDatanodes(int minFinalizedDatanodes) {
      this.minFinalizedDatanodes = minFinalizedDatanodes;
      return this;
    }

    public Builder setPipelineRequirements(PipelineRequirements pipelineRequirements) {
      this.pipelineRequirements = pipelineRequirements;
      return this;
    }

    public HDDSLayoutFeatureRequirements build() {
      return new HDDSLayoutFeatureRequirements(this);
    }
  }
}
