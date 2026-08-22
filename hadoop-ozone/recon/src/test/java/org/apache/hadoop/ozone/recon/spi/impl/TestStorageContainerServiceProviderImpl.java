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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.ha.InterSCMGrpcClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.security.ReconCertificateClient;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

/**
 * Class to test StorageContainerServiceProviderImpl APIs.
 */
public class TestStorageContainerServiceProviderImpl {

  private Injector injector;
  private HddsProtos.PipelineID pipelineID;

  @BeforeEach
  void setup(@TempDir File testDir) {
    injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        try {
          StorageContainerLocationProtocol mockScmClient = mock(
              StorageContainerLocationProtocol.class);
          ReconUtils reconUtils =  new ReconUtils();
          OzoneConfiguration conf = new OzoneConfiguration();
          conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
          pipelineID = PipelineID.randomId().getProtobuf();
          when(mockScmClient.getPipeline(pipelineID))
              .thenReturn(mock(Pipeline.class));
          bind(StorageContainerLocationProtocol.class)
              .toInstance(mockScmClient);
          bind(StorageContainerServiceProvider.class)
              .to(StorageContainerServiceProviderImpl.class);
          bind(OzoneConfiguration.class).
              toInstance(conf);
          bind(ReconUtils.class).toInstance(reconUtils);
        } catch (Exception e) {
          fail();
        }
      }
    });
  }

  @Test
  public void testGetPipelines() throws IOException {
    StorageContainerServiceProvider scmProvider =
        injector.getInstance(StorageContainerServiceProvider.class);
    StorageContainerLocationProtocol scmClient =
        injector.getInstance(StorageContainerLocationProtocol.class);
    scmProvider.getPipelines();
    verify(scmClient, times(1)).listPipelines();
  }

  @Test
  public void testGetPipeline() throws IOException {
    StorageContainerServiceProvider scmProvider =
        injector.getInstance(StorageContainerServiceProvider.class);
    StorageContainerLocationProtocol scmClient =
        injector.getInstance(StorageContainerLocationProtocol.class);
    Pipeline pipeline = scmProvider.getPipeline(pipelineID);
    assertNotNull(pipeline);
    verify(scmClient, times(1))
        .getPipeline(pipelineID);
  }

  /**
   * Recon downloads the SCM snapshot from whichever peer reports itself as
   * LEADER, using the resolved address carried in the encoded role string.
   */
  @Test
  public void testGetSCMDBSnapshotConnectsToLeader() throws Exception {
    StorageContainerServiceProvider scmProvider =
        injector.getInstance(StorageContainerServiceProvider.class);
    StorageContainerLocationProtocol scmClient =
        injector.getInstance(StorageContainerLocationProtocol.class);

    when(scmClient.getScmInfo()).thenReturn(new ScmInfo.Builder()
        .setClusterId("CID-6a1b5b1e-3f5a-4b5c-8f6d-2a1c3e4f5a6b")
        .setScmId("scm1")
        .setPeerRoles(Arrays.asList(
            "[2001:db8::2]:9894:FOLLOWER:61b1c8e5-da40-4567-8a17-96a0234ba14e:[2001:db8:0:0:0:0:0:2]",
            "[2001:db8::1]:9894:LEADER:e428ca07-b2a3-4756-bf9b-a4abb033c7d1:[2001:db8:0:0:0:0:0:1]"))
        .build());

    AtomicReference<String> downloadHost = new AtomicReference<>();
    try (MockedStatic<HddsServerUtil> stubbedServerUtil = mockStatic(HddsServerUtil.class);
         MockedConstruction<ReconCertificateClient> stubbedCertClient =
             mockConstruction(ReconCertificateClient.class);
         MockedConstruction<InterSCMGrpcClient> downloaders =
             mockConstruction(InterSCMGrpcClient.class, (downloader, context) -> {
               downloadHost.set((String) context.arguments().get(0));
               when(downloader.download(any()))
                   .thenReturn(CompletableFuture.completedFuture(null));
             })) {
      scmProvider.getSCMDBSnapshot();

      assertEquals(1, downloaders.constructed().size());
    }

    // The follower is listed first, so this also pins that the LEADER entry is
    // the one selected. The resolved literal is handed over verbatim, unbracketed.
    assertEquals("2001:db8:0:0:0:0:0:1", downloadHost.get());
  }

}
