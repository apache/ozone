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

import static org.apache.hadoop.ozone.insight.datanode.PipelineComponentUtil.getPipelineIdFromFilters;
import static org.apache.hadoop.ozone.insight.datanode.PipelineComponentUtil.withDatanodesFromPipeline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.InsightPoint;
import org.apache.hadoop.ozone.insight.LoggerSource;

/**
 * Insight definition for datanode/pipeline metrics.
 */
public class RatisInsight extends BaseInsightPoint implements InsightPoint {

  private OzoneConfiguration conf;

  public RatisInsight(OzoneConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose,
      Map<String, String> filters) {

    List<LoggerSource> result = new ArrayList<>();

    try (ScmClient scmClient = createScmClient(conf)) {
      withDatanodesFromPipeline(scmClient,
          getPipelineIdFromFilters(filters),
          dn -> {
            result
                .add(new LoggerSource(dn,
                    "org.apache.ratis.server",
                    defaultLevel(verbose)));
            return null;
          });
    } catch (IOException e) {
      throw new UncheckedIOException("Can't enumerate required logs", e);
    }
    return result;
  }

  @Override
  public String getDescription() {
    return "More information about one ratis datanode ring.";
  }

  @Override
  public boolean filterLog(Map<String, String> filters, String logLine) {
    return true;
  }
}
