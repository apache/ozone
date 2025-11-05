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

package org.apache.hadoop.ozone.admin.diskbalancer;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import picocli.CommandLine.Command;

/**
 * DiskBalancer command group for managing disk space balancing on Ozone datanodes.
 *
 * <p>The balancer is a tool that balances space usage on an Ozone datanode
 * when some disks become full or when new empty disks were added to a datanode.
 *
 * <h2>SYNOPSIS</h2>
 * <pre>
 * To start:
 *      ozone admin datanode diskbalancer start -d {@literal <host[:port]>}
 *      [ -t/--threshold {@literal <threshold>}]
 *      [ -b/--bandwidth-in-mb {@literal <bandwidthInMB>}]
 *      [ -p/--parallel-thread {@literal <parallelThread>}]
 *      [ -s/--stop-after-disk-even {@literal <stopAfterDiskEven>}]
 *
 *      Examples:
 *      ozone admin datanode diskbalancer start -d DN-1
 *        Start balancer on DN-1 using default port (9858)
 *
 *      ozone admin datanode diskbalancer start -d 192.168.1.10:9858
 *        Start balancer with explicit port specification
 *
 *      ozone admin datanode diskbalancer start -d DN-1,DN-2,DN-3
 *        Start balancer on multiple datanodes (using default port)
 *
 *      ozone admin datanode diskbalancer start --datanodes DN-1:9858,DN-2:9999
 *        Start balancer on multiple datanodes with explicit ports
 *
 *      ozone admin datanode diskbalancer start -d DN-1 -t 5
 *        Start balancer with a threshold of 5%
 *
 *      ozone admin datanode diskbalancer start -d 192.168.1.10 -b 20
 *        Start balancer with maximum 20MB/s disk bandwidth
 *
 *      ozone admin datanode diskbalancer start -d DN-1 -p 5
 *        Start balancer with 5 parallel threads
 *
 *      ozone admin datanode diskbalancer start -d DN-1 -s false
 *        Start balancer and keep running even after disks are balanced
 *
 * To stop:
 *      ozone admin datanode diskbalancer stop -d {@literal <host[:port]>}
 *
 *      Examples:
 *      ozone admin datanode diskbalancer stop -d DN-1
 *        Stop diskbalancer on DN-1 (using default port)
 *
 *      ozone admin datanode diskbalancer stop --datanodes DN-1,DN-2,DN-3
 *        Stop diskbalancer on multiple datanodes
 *
 * To update:
 *      ozone admin datanode diskbalancer update -d {@literal <host[:port]>}
 *      [ -t/--threshold {@literal <threshold>}]
 *      [ -b/--bandwidth-in-mb {@literal <bandwidthInMB>}]
 *      [ -p/--parallel-thread {@literal <parallelThread>}]
 *      [ -s/--stop-after-disk-even {@literal <stopAfterDiskEven>}]
 *
 *      Examples:
 *      ozone admin datanode diskbalancer update -d DN-1 -t 10
 *        Update diskbalancer threshold to 10% on DN-1
 *
 * To get report:
 *      ozone admin datanode diskbalancer report -d {@literal <host[:port]>}
 *
 *      Examples:
 *      ozone admin datanode diskbalancer report -d DN-1
 *        Retrieve volume density report from DN-1
 *
 *      ozone admin datanode diskbalancer report --datanodes DN-1,DN-2,DN-3
 *        Retrieve volume density report from multiple datanodes
 *
 * To get status:
 *      ozone admin datanode diskbalancer status -d {@literal <host[:port]>}
 *
 *      Examples:
 *      ozone admin datanode diskbalancer status -d DN-1
 *        Return the diskbalancer status on DN-1
 *
 *      ozone admin datanode diskbalancer status --datanodes DN-1,DN-2,DN-3
 *        Return the diskbalancer status on multiple datanodes
 *
 * </pre>
 */

@Command(
    name = "diskbalancer",
    description = "DiskBalancer specific operations. It is disabled by default." +
        " To enable it, set 'hdds.datanode.disk.balancer.enabled' as true",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        DiskBalancerStartSubcommand.class,
        DiskBalancerStopSubcommand.class,
        DiskBalancerUpdateSubcommand.class,
        DiskBalancerReportSubcommand.class,
        DiskBalancerStatusSubcommand.class
    })
public class DiskBalancerCommands {
}
