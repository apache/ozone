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

package org.apache.hadoop.ozone.shell.volume;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import picocli.CommandLine.Command;

/**
 * Subcommand to group volume related operations.
 */
@Command(name = "volume",
    aliases = "vol",
    description = "Volume specific operations",
    subcommands = {
        InfoVolumeHandler.class,
        ListVolumeHandler.class,
        CreateVolumeHandler.class,
        UpdateVolumeHandler.class,
        DeleteVolumeHandler.class,
        AddAclVolumeHandler.class,
        RemoveAclVolumeHandler.class,
        SetAclVolumeHandler.class,
        GetAclVolumeHandler.class,
        SetQuotaHandler.class,
        ClearQuotaHandler.class
    },
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class VolumeCommands {

}
