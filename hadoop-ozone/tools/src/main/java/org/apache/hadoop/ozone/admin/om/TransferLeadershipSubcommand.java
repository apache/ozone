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
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Handler of om roles command.
 */
@CommandLine.Command(
    name = "transferleadership", aliases = "transferleadership",
    description = "Transfer leadership of OM",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class TransferLeadershipSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-id", "--service-id"},
      description = "OM Service ID",
      required = true)
  private String omServiceId;

  @CommandLine.Option(names = {"--transfer-om-id"},
      description = "The OM ID to be transferring leadership to",
      required = true)
  private String transferOMId;

  private OzoneManagerProtocol ozoneManagerClient;

  @Override
  public Void call() throws Exception {
    boolean result = false;
    try {
      ozoneManagerClient =  parent.createOmClient(omServiceId);
      result = ozoneManagerClient.transferLeadership(transferOMId);
    } catch (OzoneClientException ex) {
      System.out.printf("Error: %s", ex.getMessage());
    } finally {
      if (ozoneManagerClient != null) {
        ozoneManagerClient.close();
      }
    }
    if (result) {
      System.out.println(
          "Successfully transferred leadership to " + transferOMId);
    } else {
      System.out.println("Failed to transfer leadership to " + transferOMId);
    }
    return null;
  }
}
