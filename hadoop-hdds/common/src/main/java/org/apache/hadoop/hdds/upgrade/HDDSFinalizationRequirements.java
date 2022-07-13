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

import java.util.Collection;

import static org.apache.hadoop.hdds.upgrade.HDDSFinalizationRequirements.PipelineRequirements.CLOSE_ALL_PIPELINES;

/**
 * Used by layout features in {@link HDDSLayoutFeature} to specify
 * requirements that SCM must enforce before, during, or after they finalize.
 */
public class HDDSFinalizationRequirements {
  /**
   * What each layout feature requires for pipelines while it is
   * finalizing.
   */
  public enum PipelineRequirements {
    /**
     * The layout feature requires all pipelines to be closed while finalizing.
     */
    CLOSE_ALL_PIPELINES,
    /**
     * The layout feature has no special requirements for pipeline handling
     * while it is finalizing.
     */
    NONE
  }

  private final int minFinalizedDatanodes;
  private final PipelineRequirements pipelineRequirements;

  private HDDSFinalizationRequirements(Builder builder) {
    minFinalizedDatanodes = builder.minFinalizedDatanodes;
    pipelineRequirements = builder.pipelineRequirements;
  }

  /**
   * Generates one requirements object by aggregating multiple requirements.
   * The requirements aggregate will reflect the strictest requirements of
   * any individual requirements provided.
   */
  public HDDSFinalizationRequirements(
      Collection<HDDSFinalizationRequirements> requirements) {
    int currentMinFinalizedDatanodes = 0;
    PipelineRequirements currentPipelineRequirements =
        PipelineRequirements.NONE;

    for (HDDSFinalizationRequirements req: requirements) {
      // The minimum number of datanodes we must wait to finalize is the
      // largest of the minimums of all layout features.
      currentMinFinalizedDatanodes = Math.max(currentMinFinalizedDatanodes,
          req.minFinalizedDatanodes);

      if (req.pipelineRequirements == CLOSE_ALL_PIPELINES) {
        currentPipelineRequirements = CLOSE_ALL_PIPELINES;
      }
    }

    minFinalizedDatanodes = currentMinFinalizedDatanodes;
    pipelineRequirements = currentPipelineRequirements;
  }

  /**
   * @return The minimum number of datanodes that SCM must wait to have
   * finalized before declaring finalization has finished. The remaining
   * datanodes will finalize asynchronously.
   */
  public int getMinFinalizedDatanodes() {
    return minFinalizedDatanodes;
  }

  public PipelineRequirements getPipelineRequirements() {
    return pipelineRequirements;
  }

  @Override
  public String toString() {
    return String.format("Pipeline requirements: %s\nMinimum number of " +
        "finalized datanodes: %s", pipelineRequirements, minFinalizedDatanodes);
  }

  /**
   * Builds an {@link HDDSFinalizationRequirements} object, using default
   * values for unspecified requirements.
   */
  public static final class Builder {
    private int minFinalizedDatanodes;
    private PipelineRequirements pipelineRequirements;

    public Builder() {
      // Default values.
      this.minFinalizedDatanodes = 3;
      this.pipelineRequirements = PipelineRequirements.NONE;
    }

    public Builder setMinFinalizedDatanodes(int minFinalizedDatanodes) {
      this.minFinalizedDatanodes = minFinalizedDatanodes;
      return this;
    }

    public Builder setPipelineRequirements(
        PipelineRequirements pipelineRequirements) {
      this.pipelineRequirements = pipelineRequirements;
      return this;
    }

    public HDDSFinalizationRequirements build() {
      return new HDDSFinalizationRequirements(this);
    }
  }
}
