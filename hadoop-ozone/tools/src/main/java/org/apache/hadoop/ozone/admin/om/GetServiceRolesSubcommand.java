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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Handler of om get-service-roles command.
 */
@CommandLine.Command(
    name = "getserviceroles",
    description = "List all OMs and their respective Ratis server roles",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class GetServiceRolesSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID",
      required = true)
  private String omServiceId;

  @Override
  public Void call() throws Exception {
    try {
      ClientProtocol client = parent.createClient(omServiceId);
      getOmServerRoles(client.getOmRoleInfos());
    } catch (OzoneClientException ex) {
      System.out.printf("Error: %s", ex.getMessage());
    }
    return null;
  }

  private void getOmServerRoles(List<OMRoleInfo> roleInfos) {
    for (OMRoleInfo roleInfo : roleInfos) {
      System.out.println(
          roleInfo.getNodeId() + " : " + roleInfo.getServerRole());
    }
  }
}
