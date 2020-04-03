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

package org.apache.hadoop.ozone.web.ozShell.volume;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.apache.hadoop.ozone.web.ozShell.ListOptions;
import org.apache.hadoop.ozone.web.ozShell.OzoneAddress;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.security.UserGroupInformation;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Executes List Volume call.
 */
@Command(name = "list",
    aliases = "ls",
    description = "List the volumes of a given user")
public class ListVolumeHandler extends Handler {

  @Parameters(arity = "1..1",
      description = Shell.OZONE_URI_DESCRIPTION,
      defaultValue = "/")
  private String uri;

  @CommandLine.Mixin
  private ListOptions listOptions;

  @Option(names = {"--user", "-u"},
      description = "Owner of the volumes to list.")
  private String userName;

  @Override
  protected OzoneAddress getAddress() throws OzoneClientException {
    OzoneAddress address = new OzoneAddress(uri);
    address.ensureRootAddress();
    return address;
  }

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    if (userName == null) {
      userName = UserGroupInformation.getCurrentUser().getUserName();
    }

    Iterator<? extends OzoneVolume> volumeIterator;
    if (userName != null) {
      volumeIterator = client.getObjectStore().listVolumesByUser(userName,
          listOptions.getPrefix(), listOptions.getStartItem());
    } else {
      volumeIterator = client.getObjectStore().listVolumes(
          listOptions.getPrefix());
    }

    int counter = 0;
    while (listOptions.getLimit() > counter && volumeIterator.hasNext()) {
      printObjectAsJson(volumeIterator.next());
      counter++;
    }

    if (isVerbose()) {
      out().printf("Found : %d volumes for user : %s ", counter,
          userName);
    }
  }
}

