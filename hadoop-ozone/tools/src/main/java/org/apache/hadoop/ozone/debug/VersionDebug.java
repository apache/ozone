/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.debug;

import com.google.common.collect.ImmutableSortedMap;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.DatanodeVersion;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

/** Show internal component version information as JSON. */
@CommandLine.Command(
    name = "version",
    description = "Show internal version of Ozone components.")
@MetaInfServices(SubcommandWithParent.class)
public class VersionDebug implements Callable<Void>, SubcommandWithParent {

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  @Override
  public Void call() throws IOException {
    Map<String, Map<String, Object>> versions = ImmutableSortedMap.of(
        "client", asMap(ClientVersion.CURRENT),
        "datanode", asMap(DatanodeVersion.CURRENT),
        "om", asMap(OzoneManagerVersion.CURRENT)
    );
    System.out.println(JsonUtils.toJsonStringWithDefaultPrettyPrinter(versions));
    return null;
  }

  private static <T extends Enum<T> & ComponentVersion> Map<String, Object> asMap(T version) {
    return ImmutableSortedMap.of(
        "name", version.name(),
        "protoValue", version.toProtoValue()
    );
  }

}
