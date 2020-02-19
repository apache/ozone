/**
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

package org.apache.hadoop.ozone.recon.scm;

import java.util.List;

import org.apache.commons.collections.map.DefaultedMap;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineFactory;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to stub out SCM's pipeline providers. This makes sure Recon can
 * never be on the pipeline CREATE or CLOSE path.
 */
public class ReconPipelineFactory extends PipelineFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconPipelineFactory.class);

  ReconPipelineFactory() {
    ReconPipelineProvider reconMockPipelineProvider =
        new ReconPipelineProvider();
    setProviders(new DefaultedMap(reconMockPipelineProvider));
  }

  static class ReconPipelineProvider implements PipelineProvider {

    @Override
    public Pipeline create(HddsProtos.ReplicationFactor factor){
      throw new RuntimeException(
          "Trying to create pipeline in Recon, which is prohibited!");
    }

    @Override
    public Pipeline create(HddsProtos.ReplicationFactor factor,
                           List<DatanodeDetails> nodes) {
      LOG.warn("Trying to create pipeline in Recon, which is prohibited!");
      return null;
    }

    @Override
    public void close(Pipeline pipeline) {

    }

    @Override
    public void shutdown() {

    }
  }
}
