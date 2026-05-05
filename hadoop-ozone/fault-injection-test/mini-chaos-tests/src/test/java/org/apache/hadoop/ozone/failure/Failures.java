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

package org.apache.hadoop.ozone.failure;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneChaosCluster;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public abstract void validateFailure(MiniOzoneChaosCluster cluster);

  public static List<Class<? extends Failures>> getClassList() {
    List<Class<? extends Failures>> classList = new ArrayList<>();

    classList.add(OzoneManagerRestartFailure.class);
    classList.add(OzoneManagerStartStopFailure.class);
    classList.add(DatanodeRestartFailure.class);
    classList.add(DatanodeStartStopFailure.class);
    classList.add(StorageContainerManagerStartStopFailure.class);
    classList.add(StorageContainerManagerRestartFailure.class);

    return classList;
  }

  /**
   * Ozone Manager failures.
   */
  public abstract static class OzoneFailures extends Failures {
    @Override
    public void validateFailure(MiniOzoneChaosCluster cluster) {
      if (cluster.getOzoneManagersList().size() < 3) {
        throw new IllegalArgumentException("Not enough number of " +
            "OzoneManagers to test chaos on OzoneManagers. Set number of " +
            "OzoneManagers to at least 3");
      }
    }
  }

  /**
   * Restart Ozone Manager to induce failure.
   */
  public static class OzoneManagerRestartFailure extends OzoneFailures {
    @Override
    public void fail(MiniOzoneChaosCluster cluster) {
      boolean failureMode = FailureManager.isFastRestart();
      Set<OzoneManager> oms = cluster.omToFail();
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

  /**
   * Start/Stop Ozone Manager to induce failure.
   */
  public static class OzoneManagerStartStopFailure extends OzoneFailures {
    @Override
    public void fail(MiniOzoneChaosCluster cluster) {
      // Get the number of OzoneManager to fail in the cluster.
      boolean shouldStop = cluster.shouldStopOm();
      Set<OzoneManager> oms = cluster.omToFail();
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

  /**
   * Ozone Manager failures.
   */
  public abstract static class ScmFailures extends Failures {
    @Override
    public void validateFailure(MiniOzoneChaosCluster cluster) {
      if (cluster.getStorageContainerManagersList().size() < 3) {
        throw new IllegalArgumentException("Not enough number of " +
            "StorageContainerManagers to test chaos on" +
            "StorageContainerManagers. Set number of " +
            "StorageContainerManagers to at least 3");
      }
    }
  }

  /**
   * Start/Stop Ozone Manager to induce failure.
   */
  public static class StorageContainerManagerStartStopFailure
      extends ScmFailures {
    @Override
    public void fail(MiniOzoneChaosCluster cluster) {
      // Get the number of OzoneManager to fail in the cluster.
      boolean shouldStop = cluster.shouldStopScm();
      Set<StorageContainerManager> scms = cluster.scmToFail();
      scms.parallelStream().forEach(scm -> {
        try {
          if (shouldStop) {
            // start another OM before failing the next one.
            cluster.shutdownStorageContainerManager(scm);
          } else {
            cluster.restartStorageContainerManager(scm, true);
          }
        } catch (Throwable t) {
          LOG.error("Failed to shutdown OM {}", scm, t);
        }
      });
    }
  }

  /**
   * Start/Stop Ozone Manager to induce failure.
   */
  public static class StorageContainerManagerRestartFailure
      extends ScmFailures {
    @Override
    public void fail(MiniOzoneChaosCluster cluster) {
      boolean failureMode = FailureManager.isFastRestart();
      Set<StorageContainerManager> scms = cluster.scmToFail();
      scms.parallelStream().forEach(scm -> {
        try {
          cluster.shutdownStorageContainerManager(scm);
          cluster.restartStorageContainerManager(scm, failureMode);
          cluster.waitForClusterToBeReady();
        } catch (Throwable t) {
          LOG.error("Failed to restartNodes SCM {}", scm, t);
        }
      });
    }
  }

  /**
   * Datanode failures.
   */
  public abstract static class DatanodeFailures extends Failures {
    @Override
    public void validateFailure(MiniOzoneChaosCluster cluster) {
      // Nothing to do here.
    }
  }

  /**
   * Restart Datanodes to induce failure.
   */
  public static class DatanodeRestartFailure extends DatanodeFailures {
    @Override
    public void fail(MiniOzoneChaosCluster cluster) {
      boolean failureMode = FailureManager.isFastRestart();
      Set<DatanodeDetails> dns = cluster.dnToFail();
      dns.parallelStream().forEach(dn -> {
        try {
          cluster.restartHddsDatanode(dn, failureMode);
        } catch (Throwable t) {
          LOG.error("Failed to restartNodes Datanode {}", dn.getUuid(), t);
        }
      });
    }
  }

  /**
   * Start/Stop Datanodes to induce failure.
   */
  public static class DatanodeStartStopFailure extends DatanodeFailures {
    @Override
    public void fail(MiniOzoneChaosCluster cluster) {
      // Get the number of datanodes to fail in the cluster.
      Set<DatanodeDetails> dns = cluster.dnToFail();
      dns.parallelStream().forEach(dn -> {
        try {
          if (cluster.shouldStop(dn)) {
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
