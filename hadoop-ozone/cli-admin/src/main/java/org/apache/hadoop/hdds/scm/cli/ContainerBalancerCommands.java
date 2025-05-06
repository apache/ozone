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

package org.apache.hadoop.hdds.scm.cli;

import org.apache.hadoop.hdds.cli.AdminSubcommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine.Command;

/**
 * Subcommand to group container balancer related operations.
 *
 * <p>The balancer is a tool that balances datanode space usage on an Ozone
 * cluster when some datanodes become full or when new empty nodes join
 * the cluster. The tool can be run by the cluster administrator
 * from command line while applications adding and deleting blocks.
 *
 * <p>SYNOPSIS
 * <pre>
 * To start:
 *      ozone admin containerbalancer start
 *      [ -t/--threshold {@literal <threshold>}]
 *      [ -i/--iterations {@literal <iterations>}]
 *      [ -d/--maxDatanodesPercentageToInvolvePerIteration
 *      {@literal <maxDatanodesPercentageToInvolvePerIteration>}]
 *      [ -s/--maxSizeToMovePerIterationInGB
 *      {@literal <maxSizeToMovePerIterationInGB>}]
 *      Examples:
 *      ozone admin containerbalancer start
 *        start balancer with default values in the configuration
 *      ozone admin containerbalancer start -t 5
 *        start balancer with a threshold of 5%
 *      ozone admin containerbalancer start -i 20
 *        start balancer with maximum 20 consecutive iterations
 *      ozone admin containerbalancer start -i -1
 *        run balancer infinitely with default values in the configuration
 *      ozone admin containerbalancer start -d 40
 *        start balancer with maximum 40% of healthy, in-service datanodes
 *        involved in balancing
 *      ozone admin containerbalancer start -s 10
 *        start balancer with maximum size of 10GB to move in one iteration
 * To stop:
 *      ozone admin containerbalancer stop
 * </pre>
 *
 * <p>DESCRIPTION
 * <p>The threshold parameter is a fraction in the range of (1%, 100%) with a
 * default value of 10%. The threshold sets a target for whether the cluster
 * is balanced. A cluster is balanced if for each datanode, the utilization
 * of the node (ratio of used space at the node to total capacity of the node)
 * differs from the utilization of the (ratio of used space in the cluster
 * to total capacity of the cluster) by no more than the threshold value.
 * The smaller the threshold, the more balanced a cluster will become.
 * It takes more time to run the balancer for small threshold values.
 * Also for a very small threshold the cluster may not be able to reach the
 * balanced state when applications write and delete files concurrently.
 *
 * <p>The administrator can interrupt the execution of the balancer at any
 * time by running the command "ozone admin containerbalancer stop"
 * through command line
 */
@Command(
    name = "containerbalancer",
    description = "ContainerBalancer specific operations",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        ContainerBalancerStartSubcommand.class,
        ContainerBalancerStopSubcommand.class,
        ContainerBalancerStatusSubcommand.class
    })
@MetaInfServices(AdminSubcommand.class)
public class ContainerBalancerCommands implements AdminSubcommand {

}
