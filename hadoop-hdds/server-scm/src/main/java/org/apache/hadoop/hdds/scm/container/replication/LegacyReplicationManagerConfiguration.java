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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;

import java.time.Duration;

import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;

/**
 * Configuration used by the {@link LegacyReplicationManager}.
 */
@ConfigGroup(prefix = "hdds.scm.replication")
public class LegacyReplicationManagerConfiguration {
    /**
     * The frequency in which ReplicationMonitor thread should run.
     */
    @Config(key = "thread.interval",
            type = ConfigType.TIME,
            defaultValue = "300s",
            tags = {SCM, OZONE},
            description = "There is a replication monitor thread running inside " +
                    "SCM which takes care of replicating the containers in the " +
                    "cluster. This property is used to configure the interval in " +
                    "which that thread runs."
    )
    private long interval = Duration.ofSeconds(300).toMillis();

    /**
     * Timeout for container replication & deletion command issued by
     * ReplicationManager.
     */
    @Config(key = "event.timeout",
            type = ConfigType.TIME,
            defaultValue = "30m",
            tags = {SCM, OZONE},
            description = "Timeout for the container replication/deletion commands "
                    + "sent  to datanodes. After this timeout the command will be "
                    + "retried.")
    private long eventTimeout = Duration.ofMinutes(30).toMillis();

    public void setInterval(Duration interval) {
        this.interval = interval.toMillis();
    }

    public void setEventTimeout(Duration timeout) {
        this.eventTimeout = timeout.toMillis();
    }

    /**
     * The number of container replica which must be available for a node to
     * enter maintenance.
     */
    @Config(key = "maintenance.replica.minimum",
            type = ConfigType.INT,
            defaultValue = "2",
            tags = {SCM, OZONE},
            description = "The minimum number of container replicas which must " +
                    " be available for a node to enter maintenance. If putting a " +
                    " node into maintenance reduces the available replicas for any " +
                    " container below this level, the node will remain in the " +
                    " entering maintenance state until a new replica is created.")
    private int maintenanceReplicaMinimum = 2;

    @Config(key = "container.inflight.replication.limit",
            type = ConfigType.INT,
            defaultValue = "0", // 0 means unlimited.
            tags = {SCM, OZONE},
            description = "This property is used to limit" +
                    " the maximum number of inflight replication."
    )
    private int containerInflightReplicationLimit = 0;

    @Config(key = "container.inflight.deletion.limit",
            type = ConfigType.INT,
            defaultValue = "0", // 0 means unlimited.
            tags = {SCM, OZONE},
            description = "This property is used to limit" +
                    " the maximum number of inflight deletion."
    )
    private int containerInflightDeletionLimit = 0;

    public void setContainerInflightReplicationLimit(int replicationLimit) {
        this.containerInflightReplicationLimit = replicationLimit;
    }

    public void setContainerInflightDeletionLimit(int deletionLimit) {
        this.containerInflightDeletionLimit = deletionLimit;
    }

    public void setMaintenanceReplicaMinimum(int replicaCount) {
        this.maintenanceReplicaMinimum = replicaCount;
    }

    public int getContainerInflightReplicationLimit() {
        return containerInflightReplicationLimit;
    }

    public int getContainerInflightDeletionLimit() {
        return containerInflightDeletionLimit;
    }

    public long getInterval() {
        return interval;
    }

    public long getEventTimeout() {
        return eventTimeout;
    }

    public int getMaintenanceReplicaMinimum() {
        return maintenanceReplicaMinimum;
    }
}
