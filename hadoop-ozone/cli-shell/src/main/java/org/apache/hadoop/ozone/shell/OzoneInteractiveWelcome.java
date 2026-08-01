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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.VersionInfo;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;

/**
 * Startup banner for {@code ozone interactive}.
 */
final class OzoneInteractiveWelcome {

  private OzoneInteractiveWelcome() {
  }

  static List<String> lines() {
    OzoneConfiguration conf = new OzoneConfiguration();
    VersionInfo ozone = OzoneVersionInfo.OZONE_VERSION_INFO;
    List<String> lines = new ArrayList<>();
    lines.add(String.format("Apache Ozone Interactive Shell %s(%s)",
        ozone.getVersion(), ozone.getRelease()));
    lines.add("Using OM: " + formatOmEndpoints(conf));
    lines.add("Using SCM: " + formatScmEndpoints(conf));
    lines.add("");
    lines.add("Type 'help' for command synopsis; 'exit' or Ctrl-D to quit.");
    lines.add("Press Tab to complete subcommands; type '-' then Tab to complete options.");
    lines.add("Run 'ozone version' for full build details.");
    lines.add("");
    return lines;
  }

  private static String formatOmEndpoints(OzoneConfiguration conf) {
    try {
      Collection<String> serviceIds = conf.getTrimmedStringCollection(
          OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY);
      if (!serviceIds.isEmpty()) {
        return OmUtils.getOmHAAddressesById(conf).values().stream()
            .flatMap(List::stream)
            .map(OzoneInteractiveWelcome::formatAddress)
            .distinct()
            .collect(Collectors.joining(", "));
      }
      return formatAddress(OmUtils.getOmAddress(conf));
    } catch (RuntimeException e) {
      return "(not configured; set ozone.om.address or ozone.om.service.ids)";
    }
  }

  private static String formatScmEndpoints(OzoneConfiguration conf) {
    try {
      Collection<InetSocketAddress> addresses = HddsUtils.getScmAddressForClients(conf);
      return addresses.stream()
          .map(OzoneInteractiveWelcome::formatAddress)
          .collect(Collectors.joining(", "));
    } catch (RuntimeException e) {
      return "(not configured; set ozone.scm.client.address or ozone.scm.names)";
    }
  }

  private static String formatAddress(InetSocketAddress address) {
    return address.getHostString() + ":" + address.getPort();
  }
}
