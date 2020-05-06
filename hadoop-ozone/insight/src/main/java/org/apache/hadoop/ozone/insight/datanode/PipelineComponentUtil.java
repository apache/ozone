package org.apache.hadoop.ozone.insight.datanode;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.insight.Component;
import org.apache.hadoop.ozone.insight.Component.Type;

public class PipelineComponentUtil {

  public static final String PIPELINE_FILTER = "pipeline";

  public static String getPipelineIdFromFilters(Map<String, String> filters) {
    if (filters == null || !filters.containsKey(PIPELINE_FILTER)) {
      throw new IllegalArgumentException(PIPELINE_FILTER
          + " filter should be specified (-f " + PIPELINE_FILTER
          + "=<pipelineid)");
    }

    return filters.get(PIPELINE_FILTER);
  }

  public static void withDatanodesFromPipeline(
      ScmClient scmClient,
      String pipelineId,
      Function<Component, Void> func) throws IOException {

    Optional<Pipeline> pipelineSelection = scmClient.listPipelines()
        .stream()
        .filter(
            pipline -> pipline.getId().getId().toString().equals(pipelineId))
        .findFirst();

    if (!pipelineSelection.isPresent()) {
      throw new IllegalArgumentException("No such multi-node pipeline.");
    }
    Pipeline pipeline = pipelineSelection.get();
    for (DatanodeDetails datanode : pipeline.getNodes()) {
      Component dn =
          new Component(Type.DATANODE, datanode.getUuid().toString(),
              datanode.getHostName(), 9882);
      func.apply(dn);
    }

  }
}
