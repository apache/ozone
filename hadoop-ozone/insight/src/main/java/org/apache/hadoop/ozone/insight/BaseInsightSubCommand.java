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

package org.apache.hadoop.ozone.insight;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTPS_BIND_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_BIND_HOST_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT;
import static org.apache.hadoop.hdds.server.http.HttpServer2.HTTPS_SCHEME;
import static org.apache.hadoop.hdds.server.http.HttpServer2.HTTP_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTPS_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_HOST_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.ozone.insight.datanode.DatanodeDispatcherInsight;
import org.apache.hadoop.ozone.insight.datanode.RatisInsight;
import org.apache.hadoop.ozone.insight.om.KeyManagerInsight;
import org.apache.hadoop.ozone.insight.om.OmProtocolInsight;
import org.apache.hadoop.ozone.insight.scm.EventQueueInsight;
import org.apache.hadoop.ozone.insight.scm.NodeManagerInsight;
import org.apache.hadoop.ozone.insight.scm.ReplicaManagerInsight;
import org.apache.hadoop.ozone.insight.scm.ScmProtocolBlockLocationInsight;
import org.apache.hadoop.ozone.insight.scm.ScmProtocolContainerLocationInsight;
import org.apache.hadoop.ozone.insight.scm.ScmProtocolDatanodeInsight;
import org.apache.hadoop.ozone.insight.scm.ScmProtocolSecurityInsight;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import picocli.CommandLine;

/**
 * Parent class for all the insight subcommands.
 */
public class BaseInsightSubCommand {

  @CommandLine.ParentCommand
  private Insight insightCommand;

  public InsightPoint getInsight(OzoneConfiguration configuration,
      String selection) {
    Map<String, InsightPoint> insights = createInsightPoints(configuration);

    if (!insights.containsKey(selection)) {
      throw new RuntimeException(String
          .format("No such component; %s. Available components: %s", selection,
              insights.keySet()));
    }
    return insights.get(selection);
  }

  /**
   * Utility to get the host base on a component.
   */
  public String getHost(OzoneConfiguration conf, Component component) {
    HttpConfig.Policy policy = HttpConfig.getHttpPolicy(conf);
    String protocol = policy.isHttpsEnabled() ? HTTPS_SCHEME : HTTP_SCHEME;
    
    if (component.getHostname() != null) {
      return protocol + "://" + component.getHostname() + ":" + component.getPort();
    }
    
    String address = getComponentAddress(conf, component.getName(), policy);
    return protocol + "://" + address;
  }

  /**
   * Get the component address based on HTTP policy.
   */
  private String getComponentAddress(OzoneConfiguration conf,
      Component.Type componentType, HttpConfig.Policy policy) {
    boolean isHttpsEnabled = policy.isHttpsEnabled();
    String address;

    switch (componentType) {
    case SCM:
      if (isHttpsEnabled) {
        address = conf.get(OZONE_SCM_HTTPS_ADDRESS_KEY, OZONE_SCM_HTTP_BIND_HOST_DEFAULT + ":" +
            OZONE_SCM_HTTPS_BIND_PORT_DEFAULT);
      } else {
        address = conf.get(OZONE_SCM_HTTP_ADDRESS_KEY, OZONE_SCM_HTTP_BIND_HOST_DEFAULT + ":" +
            OZONE_SCM_HTTP_BIND_PORT_DEFAULT);
      }

      // Fallback to RPC hostname
      if (getHostOnly(address).equals(OZONE_SCM_HTTP_BIND_HOST_DEFAULT)) {
        Optional<String> scmHost = HddsUtils.getHostNameFromConfigKeys(conf,
            ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
            ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);
        if (scmHost.isPresent()) {
          return scmHost.get() + ":" + getPort(address);
        }
      }
      return address;

    case OM:
      if (isHttpsEnabled) {
        address = conf.get(OZONE_OM_HTTPS_ADDRESS_KEY, OZONE_OM_HTTP_BIND_HOST_DEFAULT + ":" +
            OZONE_OM_HTTPS_BIND_PORT_DEFAULT);
      } else {
        address = conf.get(OZONE_OM_HTTP_ADDRESS_KEY, OZONE_OM_HTTP_BIND_HOST_DEFAULT + ":" +
            OZONE_OM_HTTP_BIND_PORT_DEFAULT);
      }

      // Fallback to RPC hostname
      if (getHostOnly(address).equals(OZONE_OM_HTTP_BIND_HOST_DEFAULT)) {
        Optional<String> omHost = HddsUtils.getHostNameFromConfigKeys(conf,
            OMConfigKeys.OZONE_OM_ADDRESS_KEY);
        if (omHost.isPresent()) {
          return omHost.get() + ":" + getPort(address);
        }
      }
      return address;

    default:
      throw new IllegalArgumentException(
          "Component type is not supported: " + componentType);
    }
  }

  /**
   * Extract hostname from address string.
   * e.g. Input: "0.0.0.0:9876" -> Output: "0.0.0.0"
   */
  private String getHostOnly(String address) {
    return address.split(":", 2)[0];
  }

  /**
   * Extract port from address string.
   * e.g. Input: "0.0.0.0:9876" -> Output: "9876"
   */
  private String getPort(String address) {
    return address.split(":", 2)[1];
  }

  public Map<String, InsightPoint> createInsightPoints(
      OzoneConfiguration configuration) {
    Map<String, InsightPoint> insights = new LinkedHashMap<>();
    insights.put("scm.node-manager", new NodeManagerInsight());
    insights.put("scm.replica-manager", new ReplicaManagerInsight());
    insights.put("scm.event-queue", new EventQueueInsight());
    insights.put("scm.protocol.block-location",
        new ScmProtocolBlockLocationInsight());
    insights.put("scm.protocol.heartbeat",
        new ScmProtocolDatanodeInsight());
    insights.put("scm.protocol.container-location",
        new ScmProtocolContainerLocationInsight());
    insights.put("scm.protocol.security",
             new ScmProtocolSecurityInsight());
    insights.put("om.key-manager", new KeyManagerInsight());
    insights.put("om.protocol.client", new OmProtocolInsight());
    insights.put("datanode.pipeline", new RatisInsight(configuration));
    insights.put("datanode.dispatcher",
        new DatanodeDispatcherInsight(configuration));

    return insights;
  }

  public Insight getInsightCommand() {
    return insightCommand;
  }
}
