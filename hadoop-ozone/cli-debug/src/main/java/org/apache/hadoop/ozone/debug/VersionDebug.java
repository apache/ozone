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

package org.apache.hadoop.ozone.debug;

import com.google.common.collect.ImmutableSortedMap;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.cli.DebugSubcommand;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/** Show internal component version information as JSON. */
@CommandLine.Command(
    name = "version",
    description = "Show internal version of Ozone components, as defined in the artifacts where this command is " +
        "executed.  It does not communicate with any Ozone services.  Run the same command on different nodes to " +
        "get a cross-component view of versions.  The goal of this command is to help quickly get a glance of the " +
        "latest features supported by Ozone on the current node."
)
@MetaInfServices(DebugSubcommand.class)
public class VersionDebug implements Callable<Void>, DebugSubcommand {

  @Override
  public Void call() throws IOException {
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(ImmutableSortedMap.of(
        "ozone", ImmutableSortedMap.of(
            "revision", OzoneVersionInfo.OZONE_VERSION_INFO.getRevision(),
            "url", OzoneVersionInfo.OZONE_VERSION_INFO.getUrl(),
            "version", OzoneVersionInfo.OZONE_VERSION_INFO.getVersion()
        ),
        "components", ImmutableSortedMap.of(
            "client", asMap(ClientVersion.CURRENT),
            "datanode", asMap(HDDSVersion.CURRENT),
            "om", asMap(OzoneManagerVersion.CURRENT)
        )
    )));
    return null;
  }

  private static <T extends Enum<T> & ComponentVersion> Map<String, Object> asMap(T version) {
    return ImmutableSortedMap.of(
        "componentVersion", ImmutableSortedMap.of(
            "name", version.name(),
            "protoValue", version.toProtoValue()
        )
    );
  }

}
