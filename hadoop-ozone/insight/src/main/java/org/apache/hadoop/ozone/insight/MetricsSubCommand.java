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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import picocli.CommandLine;

/**
 * Command line interface to show metrics for a specific component.
 */
@CommandLine.Command(
    name = "metrics",
    aliases = "metric",
    description = "Show available metrics.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class MetricsSubCommand extends BaseInsightSubCommand
    implements Callable<Void> {

  @CommandLine.Option(names = "-f", description = "Define filters to scope "
      + "the output (eg. -f datanode=_1234_datanode_id)")
  private Map<String, String> filters;

  @CommandLine.Parameters(description = "Name of the insight point (use list "
      + "to check the available options)")
  private String insightName;

  @Override
  public Void call() throws Exception {
    OzoneConfiguration conf =
        getInsightCommand().getOzoneConf();
    InsightPoint insight =
        getInsight(conf, insightName);
    Set<Component> sources =
        insight.getMetrics(filters)
            .stream()
            .map(MetricGroupDisplay::getComponent)
            .collect(Collectors.toSet());
    Map<Component, List<String>> metrics = getMetrics(conf, sources);
    System.out.println(
        "Metrics for `" + insightName + "` (" + insight.getDescription() + ")");
    System.out.println();
    for (MetricGroupDisplay group : insight.getMetrics(filters)) {
      System.out.println(group.getDescription());
      System.out.println();
      for (MetricDisplay display : group.getMetrics()) {
        System.out.println("  " + display.getDescription() + ": " + selectValue(
            metrics.get(group.getComponent()), display));
      }
      System.out.println();
      System.out.println();

    }
    return null;
  }

  private Map<Component, List<String>> getMetrics(OzoneConfiguration conf,
      Collection<Component> sources) {
    Map<Component, List<String>> result = new HashMap<>();
    for (Component source : sources) {
      result.put(source, getMetrics(conf, source));
    }
    return result;
  }

  private String selectValue(List<String> metrics,
      MetricDisplay metricDisplay) {
    for (String line : metrics) {
      if (line.startsWith(metricDisplay.getId())) {
        boolean filtered = false;
        for (Entry<String, String> filter : metricDisplay.getFilter()
            .entrySet()) {
          if (!line
              .contains(filter.getKey() + "=\"" + filter.getValue() + "\"")) {
            filtered = true;
          }
        }
        if (!filtered) {
          return line.split(" ")[1];
        }
      }
    }
    return "???";
  }

  private List<String> getMetrics(OzoneConfiguration conf,
      Component component) {
    String url = getHost(conf, component) + "/prom";
    try {
      HttpURLConnection httpURLConnection = InsightHttpUtils.openConnection(url, conf);
      if (httpURLConnection == null) {
        throw new RuntimeException("Failed to connect to " + url);
      }
      String response = InsightHttpUtils.readResponse(httpURLConnection);
      if (response == null) {
        throw new RuntimeException("Empty response from " + url);
      }
      return Arrays.asList(response.split("\\r?\\n"));
    } catch (IOException e) {
      throw new RuntimeException("Can't read prometheus metrics endpoint: " + e.getMessage(), e);
    }
  }

}
