/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Tool to edit on-disk container meta.
 */
@CommandLine.Command(
    name = "deletedBlockRetryCountRenewer",
    description = "Renew deleted block transactions whose retry count is -1")
@MetaInfServices(SubcommandWithParent.class)
public class DeletedBlockRetryCountRenewer extends ScmSubcommand implements
    SubcommandWithParent {

  @CommandLine.Option(names = {"-r", "--renew"},
      required = true,
      description = "Renew the deleted block transaction retry count from" +
          " -1 to 0. By default renew all expired transactions.")
  private boolean toRenew;

  @CommandLine.Option(names = {"-l", "--list"},
      split = ",",
      description = "Renew the only given deletedBlock transaction ID list, " +
          "e.g 100,101,102.(Separated by ',')")
  private List<Long> txList;

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  @Override
  public void execute(ScmClient client) throws IOException {
    txList = txList == null ? new ArrayList<>() : txList;
    if (toRenew) {
      int count = client.renewDeletedBlockRetryCount(txList);
      System.out.println("Renewed " + count + " deleted block transactions in" +
          " SCM");
    }
  }
}
