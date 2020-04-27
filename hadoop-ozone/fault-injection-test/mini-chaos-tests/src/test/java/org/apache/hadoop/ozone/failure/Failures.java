/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.failure;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneChaosCluster;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of all the failures.
 */
public abstract class Failures {
  static final Logger LOG =
      LoggerFactory.getLogger(Failures.class);

  public String getName() {
    return this.getClass().getSimpleName();
  }

  public abstract void fail(MiniOzoneChaosCluster cluster);

  public abstract void validateFailure(List<OzoneManager> ozoneManagers,
                                       StorageContainerManager scm,
                                       List<HddsDatanodeService> hddsDatanodes);

  public abstract static class OzoneFailures extends Failures {
    @Override
    public void validateFailure(List<OzoneManager> ozoneManagers,
                                StorageContainerManager scm,
                                List<HddsDatanodeService> hddsDatanodes) {
      if (ozoneManagers.size() < 3) {
        throw new IllegalArgumentException("Not enough number of " +
            "OzoneManagers to test chaos on OzoneManagers. Set number of " +
            "OzoneManagers to at least 3");
      }
    }
  }

  public static class OzoneManagerRestartFailure extends OzoneFailures {
    public void fail(MiniOzoneChaosCluster cluster) {
      boolean failureMode = FailureManager.isFastRestart();
      List<OzoneManager> oms = cluster.omToFail();
      oms.parallelStream().forEach(om -> {
        try {
          cluster.shutdownOzoneManager(om);
          cluster.restartOzoneManager(om, failureMode);
          cluster.waitForClusterToBeReady();
        } catch (Throwable t) {
          LOG.error("Failed to restartNodes OM {}", om, t);
        }
      });
    }
  }

  public static class OzoneManagerStartStopFailure extends OzoneFailures {
    public void fail(MiniOzoneChaosCluster cluster) {
      // Get the number of OzoneManager to fail in the cluster.
      boolean shouldStop = cluster.shouldStop();
      List<OzoneManager> oms = cluster.omToFail();
      oms.parallelStream().forEach(om -> {
        try {
          if (shouldStop) {
            // start another OM before failing the next one.
            cluster.shutdownOzoneManager(om);
          } else {
            cluster.restartOzoneManager(om, true);
          }
        } catch (Throwable t) {
          LOG.error("Failed to shutdown OM {}", om, t);
        }
      });
    }
  }

  public abstract static class DatanodeFailures extends Failures {
    @Override
    public void validateFailure(List<OzoneManager> ozoneManagers,
                                StorageContainerManager scm,
                                List<HddsDatanodeService> hddsDatanodes) {
      // Nothing to do here.
    }
  }

  public static class DatanodeRestartFailure extends DatanodeFailures {
    public void fail(MiniOzoneChaosCluster cluster) {
      boolean failureMode = FailureManager.isFastRestart();
      List<DatanodeDetails> dns = cluster.nodeToFail();
      dns.parallelStream().forEach(dn -> {
        try {
          cluster.restartHddsDatanode(dn, failureMode);
        } catch (Throwable t) {
          LOG.error("Failed to restartNodes Datanode {}", dn.getUuid(), t);
        }
      });
    }
  }

  public static class DatanodeStartStopFailure extends DatanodeFailures {
    public void fail(MiniOzoneChaosCluster cluster) {
      // Get the number of datanodes to fail in the cluster.
      boolean shouldStop = FailureManager.shouldStop();
      List<DatanodeDetails> dns = cluster.nodeToFail();
      dns.parallelStream().forEach(dn -> {
        try {
          if (shouldStop) {
            cluster.shutdownHddsDatanode(dn);
          } else {
            cluster.restartHddsDatanode(dn, true);
          }
        } catch (Throwable t) {
          LOG.error("Failed to shutdown Datanode {}", dn.getUuid(), t);
        }
      });
    }
  }
}
