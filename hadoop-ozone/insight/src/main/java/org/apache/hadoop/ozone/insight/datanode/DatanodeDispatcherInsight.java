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

package org.apache.hadoop.ozone.insight.datanode;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HTTPS_BIND_PORT_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HTTP_BIND_PORT_DEFAULT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.InsightPoint;
import org.apache.hadoop.ozone.insight.LoggerSource;
import org.apache.hadoop.ozone.insight.MetricGroupDisplay;

/**
 * Insight definition for HddsDispatcher.
 */
public class DatanodeDispatcherInsight extends BaseInsightPoint
    implements InsightPoint {

  private static final String DATANODE_FILTER = "datanode";

  private OzoneConfiguration conf;

  public DatanodeDispatcherInsight(
      OzoneConfiguration conf
  ) {
    this.conf = conf;
  }

  public Component getDatanodeFromFilter(Map<String, String> filters) {
    if (filters == null || !filters.containsKey(DATANODE_FILTER)) {
      throw new IllegalArgumentException("datanode"
          + " filter should be specified (-f " + "datanode"
          + "=<host_or_ip)");
    }

    String policyStr = conf.get(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY,
        OzoneConfigKeys.OZONE_HTTP_POLICY_DEFAULT);
    HttpConfig.Policy policy = HttpConfig.Policy.fromString(policyStr);

    final int port = policy.isHttpEnabled() ?
        HDDS_DATANODE_HTTP_BIND_PORT_DEFAULT :
        HDDS_DATANODE_HTTPS_BIND_PORT_DEFAULT;

    return new Component(Type.DATANODE, null,
        filters.get("datanode"), port);
  }

  @Override
  public List<LoggerSource> getRelatedLoggers(
      boolean verbose,
      Map<String, String> filters
  ) {
    List<LoggerSource> result = new ArrayList<>();
    result.add(new LoggerSource(
        getDatanodeFromFilter(filters),
        HddsDispatcher.class.getCanonicalName(),
        defaultLevel(verbose)));
    return result;
  }

  @Override
  public List<MetricGroupDisplay> getMetrics(Map<String, String> filters) {
    List<MetricGroupDisplay> result = new ArrayList<>();

    addProtocolMessageMetrics(result, "hdds_dispatcher",
        getDatanodeFromFilter(filters),
        ContainerProtos.Type.values());

    return result;
  }

  @Override
  public String getDescription() {
    return "Datanode request dispatcher (after Ratis replication)";
  }

  @Override
  public boolean filterLog(Map<String, String> filters, String logLine) {
    return true;
  }
}
