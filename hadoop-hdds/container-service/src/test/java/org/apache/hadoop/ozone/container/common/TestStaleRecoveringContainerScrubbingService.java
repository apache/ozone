/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background.StaleRecoveringContainerScrubbingService;
import org.apache.ozone.test.TestClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.RECOVERING;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * Tests to stale recovering container scrubbing service.
 */
@RunWith(Parameterized.class)
public class TestStaleRecoveringContainerScrubbingService {

  @Rule
  public final TemporaryFolder tempDir = new TemporaryFolder();
  private String datanodeUuid;
  private OzoneConfiguration conf;
  private HddsVolume hddsVolume;

  private final ContainerLayoutVersion layout;
  private final String schemaVersion;
  private String clusterID;
  private int containerIdNum = 0;
  private MutableVolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private final TestClock testClock =
      new TestClock(Instant.now(), ZoneOffset.UTC);

  public TestStaleRecoveringContainerScrubbingService(
      ContainerTestVersionInfo versionInfo) {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerTestVersionInfo.versionParameters();
  }

  @Before
  public void init() throws IOException {
    File volumeDir = tempDir.newFolder();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volumeDir.getAbsolutePath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, volumeDir.getAbsolutePath());
    datanodeUuid = UUID.randomUUID().toString();
    clusterID = UUID.randomUUID().toString();
    hddsVolume = new HddsVolume.Builder(volumeDir.getAbsolutePath())
        .conf(conf).datanodeUuid(datanodeUuid).clusterID(clusterID).build();
    hddsVolume.format(clusterID);
    hddsVolume.createWorkingDir(clusterID, null);
    volumeSet = mock(MutableVolumeSet.class);

    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);
  }

  @After
  public void cleanup() throws IOException {
    BlockUtils.shutdownCache(conf);
  }

  /**
   * A helper method to create a number of containers of given state.
   */
  private List<Long> createTestContainers(
      ContainerSet containerSet, int num,
      ContainerProtos.ContainerDataProto.State state)
      throws StorageContainerException {
    List<Long> createdIds = new ArrayList<>();
    int end = containerIdNum + num;
    for (; containerIdNum < end; containerIdNum++) {
      testClock.fastForward(10L);
      KeyValueContainerData recoveringContainerData = new KeyValueContainerData(
          containerIdNum, layout, (long) StorageUnit.GB.toBytes(5),
          UUID.randomUUID().toString(), datanodeUuid);
      //create a container with recovering state
      recoveringContainerData.setState(state);

      KeyValueContainer recoveringKeyValueContainer =
          new KeyValueContainer(recoveringContainerData,
              conf);
      recoveringKeyValueContainer.create(
          volumeSet, volumeChoosingPolicy, clusterID);
      containerSet.addContainer(recoveringKeyValueContainer);
      createdIds.add((long) containerIdNum);
    }
    return createdIds;
  }

  @Test
  public void testScrubbingStaleRecoveringContainers()
      throws Exception {
    ContainerSet containerSet = new ContainerSet(10);
    containerSet.setClock(testClock);
    StaleRecoveringContainerScrubbingService srcss =
        new StaleRecoveringContainerScrubbingService(
            50, TimeUnit.MILLISECONDS, 10,
            Duration.ofSeconds(300).toMillis(),
            containerSet);
    testClock.fastForward(1000L);
    Map<Long, ContainerProtos.ContainerDataProto.State> containerStateMap =
            new HashMap<>();
    containerStateMap.putAll(createTestContainers(containerSet, 5, CLOSED)
            .stream().collect(Collectors.toMap(i -> i, i -> CLOSED)));

    testClock.fastForward(1000L);
    srcss.runPeriodicalTaskNow();
    //closed container should not be scrubbed
    Assert.assertTrue(containerSet.containerCount() == 5);

    containerStateMap.putAll(createTestContainers(containerSet, 5,
            RECOVERING).stream()
            .collect(Collectors.toMap(i -> i, i -> UNHEALTHY)));
    testClock.fastForward(1000L);
    srcss.runPeriodicalTaskNow();
    //recovering container should be scrubbed since recovering timeout
    Assert.assertTrue(containerSet.containerCount() == 10);
    Iterator<Container<?>> it = containerSet.getContainerIterator();
    while (it.hasNext()) {
      Container<?> entry = it.next();
      Assert.assertEquals(entry.getContainerState(),
              containerStateMap.get(entry.getContainerData().getContainerID()));
    }

    //increase recovering timeout
    containerSet.setRecoveringTimeout(2000L);
    containerStateMap.putAll(createTestContainers(containerSet, 5,
            RECOVERING).stream()
            .collect(Collectors.toMap(i -> i, i -> RECOVERING)));
    testClock.fastForward(1000L);
    srcss.runPeriodicalTaskNow();
    //recovering container should not be scrubbed
    Assert.assertTrue(containerSet.containerCount() == 15);
    it = containerSet.getContainerIterator();
    while (it.hasNext()) {
      Container<?> entry = it.next();
      Assert.assertEquals(entry.getContainerState(),
              containerStateMap.get(entry.getContainerData().getContainerID()));
    }
  }
}
