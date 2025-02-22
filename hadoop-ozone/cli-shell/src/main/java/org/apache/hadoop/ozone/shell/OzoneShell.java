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

package org.apache.hadoop.ozone.shell;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.shell.bucket.BucketCommands;
import org.apache.hadoop.ozone.shell.keys.KeyCommands;
import org.apache.hadoop.ozone.shell.prefix.PrefixCommands;
import org.apache.hadoop.ozone.shell.snapshot.SnapshotCommands;
import org.apache.hadoop.ozone.shell.tenant.TenantUserCommands;
import org.apache.hadoop.ozone.shell.token.TokenCommands;
import org.apache.hadoop.ozone.shell.volume.VolumeCommands;
import picocli.CommandLine.Command;

/**
 * Shell commands for native rpc object manipulation.
 */
@Command(name = "ozone sh",
    description = "Shell for Ozone object store",
    subcommands = {
        BucketCommands.class,
        KeyCommands.class,
        PrefixCommands.class,
        SnapshotCommands.class,
        TenantUserCommands.class,
        TokenCommands.class,
        VolumeCommands.class,
    },
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class OzoneShell extends Shell {

  public static void main(String[] argv) throws Exception {
    new OzoneShell().run(argv);
  }
}
