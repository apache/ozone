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
package org.apache.hadoop.ozone.shell.tenant;

import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.TenantInfoList;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import picocli.CommandLine;

import java.io.IOException;

/**
 * ozone tenant list.
 */
@CommandLine.Command(name = "list",
    aliases = {"ls"},
    description = "List tenants")
public class TenantListHandler extends TenantHandler {

//  @CommandLine.Mixin
//  private ListOptions listOptions;

//  @CommandLine.Option(names = {"--json", "-j"},
//      description = "Print the result in JSON.")
//  private boolean printJson;

  // TODO: long == json later.
  @CommandLine.Option(names = {"--long"},
      // Not using -l here as it potentially collides with -l inside ListOptions
      //  if we do need pagination at some point.
      description = "List in long format")
  private boolean longFormat;

  @CommandLine.Option(names = {"--header", "-H"},
      description = "Print header")
  private boolean printHeader;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address) {
    final ObjectStore objStore = client.getObjectStore();
    try {
      TenantInfoList tenantInfoList = objStore.listTenant();

      if (printHeader) {
        // default console width 80 / 5 = 16. +1 for extra room. Change later?
        out().format(longFormat ? "%-17s" : "%s%n",
            "Tenant");
        if (longFormat) {
          // TODO: rename these fields?
          // TODO: print JSON by default after rebase.
          out().format("%-17s%-17s%-17s%s%n",
              "BucketNS",
              "AccountNS",  // == Volume name IIRC ?
              "UserPolicy",
              "BucketPolicy");
        }
      }

      tenantInfoList.getTenantInfoList().forEach(tenantInfo -> {
        out().format(longFormat ? "%-17s" : "%s%n",
            tenantInfo.getTenantName());
        if (longFormat) {
          out().format("%-17s%-17s%-17s%s%n",
              tenantInfo.getBucketNamespaceName(),
              tenantInfo.getAccountNamespaceName(),
              tenantInfo.getUserPolicyGroupName(),
              tenantInfo.getBucketPolicyGroupName());
        }
      });
    } catch (IOException e) {
      LOG.error("Failed to list tenants: {}", e.getMessage());
    }
  }
}
