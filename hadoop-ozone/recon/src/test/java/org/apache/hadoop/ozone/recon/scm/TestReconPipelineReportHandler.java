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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.junit.jupiter.api.Test;

/**
 * Class to test handling of known and new pipelines by Recon's pipeline
 * report handler.
 */
public class TestReconPipelineReportHandler {

  @Test
  public void testProcessPipelineReport()
      throws IOException, TimeoutException {

    // Check with pipeline which does not exist in Recon.
    Pipeline pipeline = getRandomPipeline();
    PipelineID pipelineID = pipeline.getId();
    HddsProtos.PipelineID pipelineIDProto =  pipelineID.getProtobuf();

    ReconPipelineManager reconPipelineManagerMock = mock(
        ReconPipelineManager.class);
    when(reconPipelineManagerMock.getPipeline(pipelineID)).thenReturn(pipeline);

    StorageContainerServiceProvider scmServiceProviderMock = mock(
        StorageContainerServiceProvider.class);
    when(scmServiceProviderMock.getPipeline(pipelineIDProto))
        .thenReturn(pipeline);

    OzoneConfiguration configuration = new OzoneConfiguration();

    ReconPipelineReportHandler handler =
        new ReconPipelineReportHandler(new ReconSafeModeManager(),
            reconPipelineManagerMock, SCMContext.emptyContext(),
            configuration, scmServiceProviderMock);

    EventPublisher eventPublisherMock = mock(EventPublisher.class);
    PipelineReport report = mock(PipelineReport.class);
    when(report.getPipelineID()).thenReturn(pipelineIDProto);

    handler.processPipelineReport(report, pipeline.getNodes().get(0),
        eventPublisherMock);

    // Verify that the new pipeline was added to pipeline manager.
    verify(reconPipelineManagerMock, times(1))
        .addPipeline(pipeline);
    verify(reconPipelineManagerMock, times(1))
        .getPipeline(pipelineID);

    // Check with pipeline which already exists in Recon.
    pipeline = getRandomPipeline();
    pipelineID = pipeline.getId();
    pipelineIDProto =  pipelineID.getProtobuf();

    when(reconPipelineManagerMock.containsPipeline(pipelineID))
        .thenReturn(true);
    when(reconPipelineManagerMock.getPipeline(pipelineID))
        .thenReturn(pipeline);
    when(report.getPipelineID()).thenReturn(pipelineIDProto);

    handler.processPipelineReport(report, pipeline.getNodes().get(0),
        eventPublisherMock);

    // Verify that the pipeline was not added to pipeline manager.
    verify(reconPipelineManagerMock, times(0))
        .addPipeline(pipeline);
    verify(reconPipelineManagerMock, times(1))
        .getPipeline(pipelineID);
  }

}
