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
 * Configuration used by the {@link ReplicationManager}.
 */
@ConfigGroup(prefix = "hdds.scm.replication")
public class ReplicationManagerConfiguration {
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
     * The frequency in which the Under Replicated queue is processed.
     */
    @Config(key = "under.replicated.interval",
            type = ConfigType.TIME,
            defaultValue = "30s",
            tags = {SCM, OZONE},
            description = "How frequently to check if there are work to process " +
                    " on the under replicated queue"
    )
    private long underReplicatedInterval = Duration.ofSeconds(30).toMillis();

    /**
     * The frequency in which the Over Replicated queue is processed.
     */
    @Config(key = "over.replicated.interval",
            type = ConfigType.TIME,
            defaultValue = "30s",
            tags = {SCM, OZONE},
            description = "How frequently to check if there are work to process " +
                    " on the over replicated queue"
    )
    private long overReplicatedInterval = Duration.ofSeconds(30).toMillis();

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

    public void setMaintenanceReplicaMinimum(int replicaCount) {
        this.maintenanceReplicaMinimum = replicaCount;
    }

    /**
     * Defines how many redundant replicas of a container must be online for a
     * node to enter maintenance. Currently, only used for EC containers. We
     * need to consider removing the "maintenance.replica.minimum" setting
     * and having both Ratis and EC use this new one.
     */
    @Config(key = "maintenance.remaining.redundancy",
            type = ConfigType.INT,
            defaultValue = "1",
            tags = {SCM, OZONE},
            description = "The number of redundant containers in a group which" +
                    " must be available for a node to enter maintenance. If putting" +
                    " a node into maintenance reduces the redundancy below this value" +
                    " , the node will remain in the ENTERING_MAINTENANCE state until" +
                    " a new replica is created. For Ratis containers, the default" +
                    " value of 1 ensures at least two replicas are online, meaning 1" +
                    " more can be lost without data becoming unavailable. For any EC" +
                    " container it will have at least dataNum + 1 online, allowing" +
                    " the loss of 1 more replica before data becomes unavailable." +
                    " Currently only EC containers use this setting. Ratis containers" +
                    " use hdds.scm.replication.maintenance.replica.minimum. For EC," +
                    " if nodes are in maintenance, it is likely reconstruction reads" +
                    " will be required if some of the data replicas are offline. This" +
                    " is seamless to the client, but will affect read performance."
    )
    private int maintenanceRemainingRedundancy = 1;

    public void setMaintenanceRemainingRedundancy(int redundancy) {
        this.maintenanceRemainingRedundancy = redundancy;
    }

    public int getMaintenanceRemainingRedundancy() {
        return maintenanceRemainingRedundancy;
    }

    public long getInterval() {
        return interval;
    }

    public long getUnderReplicatedInterval() {
        return underReplicatedInterval;
    }

    public void setUnderReplicatedInterval(Duration duration) {
        this.underReplicatedInterval = duration.toMillis();
    }

    public void setOverReplicatedInterval(Duration duration) {
        this.overReplicatedInterval = duration.toMillis();
    }

    public long getOverReplicatedInterval() {
        return overReplicatedInterval;
    }

    public long getEventTimeout() {
        return eventTimeout;
    }

    public int getMaintenanceReplicaMinimum() {
        return maintenanceReplicaMinimum;
    }
}
