/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.admin.om;

import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.conf.ReconfigurationUtil;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.security.UserGroupInformation;
import picocli.CommandLine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Handler of om roles command.
 */
@CommandLine.Command(
    name = "reconfig",
    customSynopsis = "ozone admin om reconfig -address=<ip:port> -op=start|status|properties",
    description = "Dynamic reconfiguring without stopping the OM.\n" +
        "Three operations are provided:\n" +
        "\tstart:      Execute the reconfig operation asynchronously\n" +
        "\tstatus:     Check reconfiguration status\n" +
        "\tproperties: List reconfigurable properties",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ReconfigOMSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-address", "--host-port"},
      description = "om address: <ip:port> or <hostname:port>",
      required = true)
  private String address;

  @CommandLine.Option(names = {"-op"},
      description = "op",
      required = true)
  private String op;

  private OzoneConfiguration ozoneConf;
  private UserGroupInformation user;

  @Override
  public Void call() throws IOException {
    ozoneConf = new OzoneConfiguration();
    user = UserGroupInformation.getCurrentUser();

    InetSocketAddress socketAddr = NetUtils.createSocketAddr(address);
    OMNodeDetails omNode = new OMNodeDetails.Builder().setRpcAddress(socketAddr).build();
    try (OMAdminProtocolClientSideImpl omAdminProtocolClient = 
             OMAdminProtocolClientSideImpl.createProxyForSingleOM(ozoneConf, user, omNode)) {

      if ("start".equals(op)) {
        omAdminProtocolClient.startOmReconfiguration();
        System.out.printf("Started OM reconfiguration task on node [%s].\n", address);

      } else if ("status".equals(op)) {
        ReconfigurationTaskStatus status = omAdminProtocolClient.getOmReconfigurationStatus();
        System.out.printf("Reconfiguring status for node [%s]: ", address);
        printReconfigurationStatus(status);

      } else if ("properties".equals(op)) {
        List<String> properties = omAdminProtocolClient.listOmReconfigurableProperties();
        System.out.printf("OM Node [%s] Reconfigurable properties:%n", address);
        for (String name : properties) {
          System.out.println(name);
        }
      } else {
        System.err.println("Unknown operation: " + op);
      }
    } catch (IOException e) {
      System.err.printf("OM Node [%s] Reconfigure op=%s Failed: %s.", address, op, e.getMessage());
    }
    return null;
  }
  
  private void printReconfigurationStatus(ReconfigurationTaskStatus status) {
    if (!status.hasTask()) {
      System.out.println("no task was found.");
      return;
    }
    System.out.print("started at " + new Date(status.getStartTime()));
    if (!status.stopped()) {
      System.out.println(" and is still running.");
      return;
    }
    System.out.println(" and finished at " + new Date(status.getEndTime()).toString() + ".");
    if (status.getStatus() == null) {
      // Nothing to report.
      return;
    }
    for (Map.Entry<ReconfigurationUtil.PropertyChange, Optional<String>> result : status
        .getStatus().entrySet()) {
      if (!result.getValue().isPresent()) {
        System.out.printf(
            "SUCCESS: Changed OM property %s%n\tFrom: \"%s\"%n\tTo: \"%s\"%n",
            result.getKey().prop, result.getKey().oldVal, result.getKey().newVal);
      } else {
        final String errorMsg = result.getValue().get();
        System.out.printf(
            "FAILED: Change OM property %s%n\tFrom: \"%s\"%n\tTo: \"%s\"%n",
            result.getKey().prop, result.getKey().oldVal, result.getKey().newVal);
        System.out.println("\tError: " + errorMsg + ".");
      }
    }
  }
}
