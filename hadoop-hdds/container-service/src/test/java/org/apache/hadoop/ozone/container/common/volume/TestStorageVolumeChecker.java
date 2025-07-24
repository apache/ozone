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

package org.apache.hadoop.ozone.container.common.volume;

import static org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult.FAILED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.FakeTimer;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link StorageVolumeChecker}.
 */
public class TestStorageVolumeChecker {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestStorageVolumeChecker.class);

  private static final int NUM_VOLUMES = 2;

  @TempDir
  private Path folder;

  private OzoneConfiguration conf = new OzoneConfiguration();

  /**
   * When null, the check call should throw an exception.
   */
  private VolumeCheckResult expectedVolumeHealth;

  private void initTest(VolumeCheckResult result) {
    this.expectedVolumeHealth = result;
    setup();
  }

  private void setup() {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, folder.toString());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, folder.toString());
  }

  /**
   * Run each test case for each possible value of {@link VolumeCheckResult}.
   * Including "null" for 'throw exception'.
   */
  private static List<VolumeCheckResult> volumeCheckResults() {
    List<VolumeCheckResult> list = new ArrayList<>(Arrays.asList(VolumeCheckResult.values()));
    list.add(null);
    return list;
  }

  private static List<Arguments> provideTestData() {
    List<VolumeCheckResult> volumeCheckResults = volumeCheckResults();
    List<Arguments> values = new ArrayList<>();
    for (ContainerLayoutVersion layout : ContainerLayoutVersion.values()) {
      for (VolumeCheckResult result : volumeCheckResults) {
        values.add(Arguments.of(result, layout));
      }
    }
    return values;
  }


  /**
   * Test {@link StorageVolumeChecker#checkVolume} propagates the
   * check to the delegate checker.
   */
  @ParameterizedTest
  @MethodSource("volumeCheckResults")
  public void testCheckOneVolume(VolumeCheckResult checkResult) throws Exception {
    initTest(checkResult);
    final HddsVolume volume = makeVolumes(1, expectedVolumeHealth).get(0);
    final StorageVolumeChecker checker =
        new StorageVolumeChecker(new OzoneConfiguration(), new FakeTimer(), "");
    checker.setDelegateChecker(new DummyChecker());
    final AtomicLong numCallbackInvocations = new AtomicLong(0);

    /*
     * Request a check and ensure it triggered {@link HddsVolume#check}.
     */
    boolean result =
        checker.checkVolume(volume, (healthyVolumes, failedVolumes) -> {
          numCallbackInvocations.incrementAndGet();
          if (expectedVolumeHealth != null &&
              expectedVolumeHealth != FAILED) {
            assertThat(healthyVolumes.size()).isEqualTo(1);
            assertThat(failedVolumes.size()).isEqualTo(0);
          } else {
            assertThat(healthyVolumes.size()).isEqualTo(0);
            assertThat(failedVolumes.size()).isEqualTo(1);
          }
        });

    GenericTestUtils.waitFor(() -> numCallbackInvocations.get() > 0, 5, 10000);

    // Ensure that the check was invoked at least once.
    verify(volume, times(1)).check(any());
    if (result) {
      assertThat(numCallbackInvocations.get()).isEqualTo(1L);
    }

    checker.shutdownAndWait(0, TimeUnit.SECONDS);
  }

  /**
   * Test {@link StorageVolumeChecker#checkAllVolumes} propagates
   * checks for all volumes to the delegate checker.
   */
  @ParameterizedTest
  @MethodSource("volumeCheckResults")
  public void testCheckAllVolumes(VolumeCheckResult checkResult) throws Exception {
    initTest(checkResult);

    final List<HddsVolume> volumes = makeVolumes(
        NUM_VOLUMES, expectedVolumeHealth);
    final StorageVolumeChecker checker =
        new StorageVolumeChecker(new OzoneConfiguration(), new FakeTimer(), "");
    checker.setDelegateChecker(new DummyChecker());

    Set<? extends StorageVolume> failedVolumes =
        checker.checkAllVolumes(volumes);
    LOG.info("Got back {} failed volumes", failedVolumes.size());

    if (expectedVolumeHealth == null || expectedVolumeHealth == FAILED) {
      assertThat(failedVolumes.size()).isEqualTo(NUM_VOLUMES);
    } else {
      assertTrue(failedVolumes.isEmpty());
    }

    // Ensure each volume's check() method was called exactly once.
    for (HddsVolume volume : volumes) {
      verify(volume, times(1)).check(any());
    }

    checker.shutdownAndWait(0, TimeUnit.SECONDS);
  }


  /**
   * Test {@link StorageVolumeChecker#checkAllVolumes} propagates
   * checks for all volumes to the delegate checker.
   */
  @ParameterizedTest
  @MethodSource("provideTestData")
  public void testVolumeDeletion(VolumeCheckResult checkResult,
      ContainerLayoutVersion layout) throws Exception {
    initTest(checkResult);

    DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);
    dnConf.setDiskCheckMinGap(Duration.ofMillis(0));
    conf.setFromObject(dnConf);

    DatanodeDetails datanodeDetails =
        ContainerTestUtils.createDatanodeDetails();
    OzoneContainer ozoneContainer =
        ContainerTestUtils.getOzoneContainer(datanodeDetails, conf);
    MutableVolumeSet volumeSet = ozoneContainer.getVolumeSet();
    ContainerSet containerSet = ozoneContainer.getContainerSet();

    StorageVolumeChecker volumeChecker = volumeSet.getVolumeChecker();
    volumeChecker.setDelegateChecker(new DummyChecker());
    File volParentDir =
        new File(folder.toString(), UUID.randomUUID().toString());
    volumeSet.addVolume(volParentDir.getPath());
    File volRootDir = new File(volParentDir, "hdds");

    int i = 0;
    for (ContainerDataProto.State state : ContainerDataProto.State.values()) {
      if (!state.equals(ContainerDataProto.State.INVALID)) {
        // add containers to the created volume
        Container container = ContainerTestUtils.getContainer(++i, layout,
            state);
        container.getContainerData()
            .setVolume((HddsVolume) volumeSet.getVolumeMap()
                .get(volRootDir.getPath()));
        ((KeyValueContainerData) container.getContainerData())
            .setMetadataPath(volParentDir.getPath());
        containerSet.addContainer(container);
      }
    }

    // delete the volume directory
    FileUtils.deleteDirectory(volParentDir);

    assertEquals(2, volumeSet.getVolumesList().size());
    volumeSet.checkAllVolumes();
    // failed volume should be removed from volumeSet volume list
    assertEquals(1, volumeSet.getVolumesList().size());
    assertEquals(1, volumeSet.getFailedVolumesList().size());

    // All containers should be removed from containerSet
    assertEquals(0, containerSet.getContainerMap().size());

    ozoneContainer.stop();
  }

  /**
   * Test numScansSkipped metric from VolumeInfoMetrics when volume check is skipped.
   */
  @Test
  public void testNumScansSkipped() throws Exception {
    initTest(VolumeCheckResult.HEALTHY);

    final List<HddsVolume> volumes = makeVolumes(3, expectedVolumeHealth);

    FakeTimer timer = new FakeTimer();
    final StorageVolumeChecker checker =
        new StorageVolumeChecker(new OzoneConfiguration(), timer, "");

    VolumeInfoMetrics metrics1 = new VolumeInfoMetrics("test-volume-1", volumes.get(0));
    VolumeInfoMetrics metrics2 = new VolumeInfoMetrics("test-volume-2", volumes.get(1));
    VolumeInfoMetrics metrics3 = new VolumeInfoMetrics("test-volume-3", volumes.get(2));
    when(volumes.get(0).getVolumeInfoStats()).thenReturn(metrics1);
    when(volumes.get(1).getVolumeInfoStats()).thenReturn(metrics2);
    when(volumes.get(2).getVolumeInfoStats()).thenReturn(metrics3);

    checker.checkAllVolumes(volumes);

    // No volume skipped on first call
    assertEquals(0, metrics1.getNumScansSkipped());
    assertEquals(0, metrics2.getNumScansSkipped());
    assertEquals(0, metrics3.getNumScansSkipped());

    // Second call skipped because msSinceLastCheck (8 mins) < minMsBetweenChecks (10 mins)
    timer.advance(480_000);
    checker.checkAllVolumes(volumes);

    assertEquals(1, metrics1.getNumScansSkipped());
    assertEquals(1, metrics2.getNumScansSkipped());
    assertEquals(1, metrics3.getNumScansSkipped());

    // Third call should not skip volumes as msSinceLastCheck (11 mins) > minMsBetweenChecks (10 mins)
    timer.advance(180_000);
    checker.checkAllVolumes(volumes);

    assertEquals(1, metrics1.getNumScansSkipped());
    assertEquals(1, metrics2.getNumScansSkipped());
    assertEquals(1, metrics3.getNumScansSkipped());

    checker.shutdownAndWait(0, TimeUnit.SECONDS);
  }

  /**
   * A checker to wraps the result of {@link HddsVolume#check} in
   * an ImmediateFuture.
   */
  static class DummyChecker
      implements AsyncChecker<Boolean, VolumeCheckResult> {

    @Override
    public Optional<ListenableFuture<VolumeCheckResult>> schedule(
        Checkable<Boolean, VolumeCheckResult> target,
        Boolean context) {
      try {
        LOG.info("Returning success for volume check");
        return Optional.of(
            Futures.immediateFuture(target.check(context)));
      } catch (Exception e) {
        LOG.info("check routine threw exception", e);
        return Optional.of(Futures.immediateFailedFuture(e));
      }
    }

    @Override
    public void shutdownAndWait(long timeout, TimeUnit timeUnit)
        throws InterruptedException {
      // Nothing to cancel.
    }
  }

  static List<HddsVolume> makeVolumes(
      int numVolumes, VolumeCheckResult health) throws Exception {
    final List<HddsVolume> volumes = new ArrayList<>(numVolumes);
    for (int i = 0; i < numVolumes; ++i) {
      final HddsVolume volume = mock(HddsVolume.class);

      if (health != null) {
        when(volume.check(any(Boolean.class))).thenReturn(health);
        when(volume.check(isNull())).thenReturn(health);
      } else {
        final DiskErrorException de = new DiskErrorException("Fake Exception");
        when(volume.check(any(Boolean.class))).thenThrow(de);
        when(volume.check(isNull())).thenThrow(de);
      }
      volumes.add(volume);
    }
    return volumes;
  }
}
