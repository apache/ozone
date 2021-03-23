/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.checker.Checkable;
import org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.FakeTimer;
import org.junit.Rule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdfs.server.datanode.checker.VolumeCheckResult.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link HddsVolumeChecker}.
 */
@RunWith(Parameterized.class)
public class TestHddsVolumeChecker {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestHddsVolumeChecker.class);

  private static final int NUM_VOLUMES = 2;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Rule
  public Timeout globalTimeout = Timeout.seconds(30);

  private OzoneConfiguration conf = new OzoneConfiguration();

  /**
   * When null, the check call should throw an exception.
   */
  private final VolumeCheckResult expectedVolumeHealth;

  private final ChunkLayOutVersion layout;

  public TestHddsVolumeChecker(VolumeCheckResult result,
      ChunkLayOutVersion layout) {
    this.expectedVolumeHealth = result;
    this.layout = layout;
  }

  @Before
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, folder.getRoot()
        .getAbsolutePath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        folder.newFolder().getAbsolutePath());
  }

  @After
  public void cleanup() throws IOException {
    FileUtils.deleteDirectory(folder.getRoot());
  }

  /**
   * Run each test case for each possible value of {@link VolumeCheckResult}.
   * Including "null" for 'throw exception'.
   */
  @Parameters
  public static Collection<Object[]> data() {
    List<Object[]> values = new ArrayList<>();
    for (ChunkLayOutVersion layout : ChunkLayOutVersion.values()) {
      for (VolumeCheckResult result : VolumeCheckResult.values()) {
        values.add(new Object[]{result, layout});
      }
      values.add(new Object[]{null, layout});
    }
    return values;
  }


  /**
   * Test {@link HddsVolumeChecker#checkVolume} propagates the
   * check to the delegate checker.
   *
   * @throws Exception
   */
  @Test
  public void testCheckOneVolume() throws Exception {
    LOG.info("Executing {}", testName.getMethodName());
    final HddsVolume volume = makeVolumes(1, expectedVolumeHealth).get(0);
    final HddsVolumeChecker checker =
        new HddsVolumeChecker(new OzoneConfiguration(), new FakeTimer());
    checker.setDelegateChecker(new DummyChecker());
    final AtomicLong numCallbackInvocations = new AtomicLong(0);

    /**
     * Request a check and ensure it triggered {@link HddsVolume#check}.
     */
    boolean result =
        checker.checkVolume(volume, (healthyVolumes, failedVolumes) -> {
          numCallbackInvocations.incrementAndGet();
          if (expectedVolumeHealth != null &&
              expectedVolumeHealth != FAILED) {
            assertThat(healthyVolumes.size(), is(1));
            assertThat(failedVolumes.size(), is(0));
          } else {
            assertThat(healthyVolumes.size(), is(0));
            assertThat(failedVolumes.size(), is(1));
          }
        });

    GenericTestUtils.waitFor(() -> numCallbackInvocations.get() > 0, 5, 10000);

    // Ensure that the check was invoked at least once.
    verify(volume, times(1)).check(anyObject());
    if (result) {
      assertThat(numCallbackInvocations.get(), is(1L));
    }
  }

  /**
   * Test {@link HddsVolumeChecker#checkAllVolumes} propagates
   * checks for all volumes to the delegate checker.
   *
   * @throws Exception
   */
  @Test
  public void testCheckAllVolumes() throws Exception {
    LOG.info("Executing {}", testName.getMethodName());

    final List<HddsVolume> volumes = makeVolumes(
        NUM_VOLUMES, expectedVolumeHealth);
    final HddsVolumeChecker checker =
        new HddsVolumeChecker(new OzoneConfiguration(), new FakeTimer());
    checker.setDelegateChecker(new DummyChecker());

    Set<HddsVolume> failedVolumes = checker.checkAllVolumes(volumes);
    LOG.info("Got back {} failed volumes", failedVolumes.size());

    if (expectedVolumeHealth == null || expectedVolumeHealth == FAILED) {
      assertThat(failedVolumes.size(), is(NUM_VOLUMES));
    } else {
      assertTrue(failedVolumes.isEmpty());
    }

    // Ensure each volume's check() method was called exactly once.
    for (HddsVolume volume : volumes) {
      verify(volume, times(1)).check(anyObject());
    }
  }


  /**
   * Test {@link HddsVolumeChecker#checkAllVolumes} propagates
   * checks for all volumes to the delegate checker.
   *
   * @throws Exception
   */
  @Test
  public void testVolumeDeletion() throws Exception {
    LOG.info("Executing {}", testName.getMethodName());

    conf.setTimeDuration(
        DFSConfigKeysLegacy.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY, 0,
        TimeUnit.MILLISECONDS);

    DatanodeDetails datanodeDetails =
        ContainerTestUtils.createDatanodeDetails();
    OzoneContainer ozoneContainer =
        ContainerTestUtils.getOzoneContainer(datanodeDetails, conf);
    MutableVolumeSet volumeSet = ozoneContainer.getVolumeSet();
    ContainerSet containerSet = ozoneContainer.getContainerSet();

    HddsVolumeChecker volumeChecker = volumeSet.getVolumeChecker();
    volumeChecker.setDelegateChecker(new DummyChecker());
    File volParentDir =
        new File(folder.getRoot(), UUID.randomUUID().toString());
    volumeSet.addVolume(volParentDir.getPath());
    File volRootDir = new File(volParentDir, "hdds");

    int i = 0;
    for (ContainerDataProto.State state : ContainerDataProto.State.values()) {
      if (!state.equals(ContainerDataProto.State.INVALID)) {
        // add containers to the created volume
        Container container = ContainerTestUtils.getContainer(++i, layout,
            state);
        container.getContainerData()
            .setVolume(volumeSet.getVolumeMap().get(volRootDir.getPath()));
        ((KeyValueContainerData) container.getContainerData())
            .setMetadataPath(volParentDir.getPath());
        containerSet.addContainer(container);
      }
    }

    // delete the volume directory
    FileUtils.deleteDirectory(volParentDir);

    Assert.assertEquals(2, volumeSet.getVolumesList().size());
    volumeSet.checkAllVolumes();
    // failed volume should be removed from volumeSet volume list
    Assert.assertEquals(1, volumeSet.getVolumesList().size());
    Assert.assertEquals(1, volumeSet.getFailedVolumesList().size());

    i = 0;
    for (ContainerDataProto.State state : ContainerDataProto.State.values()) {
      if (!state.equals(ContainerDataProto.State.INVALID)) {
        Assert.assertEquals(ContainerDataProto.State.UNHEALTHY,
            containerSet.getContainer(++i).getContainerState());
      }
    }

    ozoneContainer.stop();
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
        LOG.info("check routine threw exception {}", e);
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