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

package org.apache.hadoop.ozone.admin.om;

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import picocli.CommandLine;

/**
 * Handler of ozone admin om cancelprepare command.
 */
@CommandLine.Command(
    name = "cancelprepare",
    description = "Deprecated. This command is no longer required, but is kept for backward compatibility with older" +
        " Ozone Managers.",
    hidden = true,
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class CancelPrepareSubCommand implements Callable<Void> {

  @CommandLine.Mixin
  private OmAddressOptions.OptionalServiceIdMixin omServiceOption;

  @Override
  public Void call() throws Exception {
    try (OzoneManagerProtocol client = omServiceOption.newClient()) {
      client.cancelOzoneManagerPrepare();
      System.out.println("Cancel prepare succeeded, cluster can now accept " +
          "write requests.");
    }
    return null;
  }
}
