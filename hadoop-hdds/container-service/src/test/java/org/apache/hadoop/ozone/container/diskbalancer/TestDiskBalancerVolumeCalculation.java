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

package org.apache.hadoop.ozone.container.diskbalancer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeDiskBalancerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.VolumeReportProto;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for disk balancer volume calculations.
 */
class TestDiskBalancerVolumeCalculation {

  @TempDir
  private Path tempDir;

  @Test
  void getIdealUsageReturnsZeroForEmptyVolumeList() {
    assertEquals(0.0, DiskBalancerVolumeCalculation.getIdealUsage(
        Collections.emptyList()));
  }

  @Test
  void isBalanceableRequiresAtLeastTwoVolumes() throws IOException {
    assertFalse(DiskBalancerVolumeCalculation.isBalanceable(null));
    assertFalse(DiskBalancerVolumeCalculation.isBalanceable(Collections.emptyList()));
    assertFalse(DiskBalancerVolumeCalculation.isBalanceable(
        Collections.singletonList(usage("solo", 100, 50))));
    assertTrue(DiskBalancerVolumeCalculation.isBalanceable(
        Arrays.asList(usage("a", 100, 50), usage("b", 100, 50))));
  }

  /**
   * The range is the ideal usage plus or minus the threshold, so a 10% threshold around a 50%
   * ideal accepts everything between 40% and 60%.
   */
  @Test
  void thresholdRangeBracketsIdealUsage() throws IOException {
    // 20 of 100 used on one volume, 80 of 100 on the other -> ideal 50%.
    List<DiskBalancerVolumeCalculation.VolumeFixedUsage> volumes =
        Arrays.asList(usage("low", 100, 80), usage("high", 100, 20));

    DiskBalancerVolumeCalculation.ThresholdRange range =
        DiskBalancerVolumeCalculation.ThresholdRange.of(volumes, 10.0);

    // Tolerance covers the small amount of space the volume builder reserves.
    assertEquals(0.5, range.getIdealUsage(), 0.01);
    assertEquals(0.4, range.getLowerThreshold(), 0.01);
    assertEquals(0.6, range.getUpperThreshold(), 0.01);
  }

  /**
   * Violation measures how far the worst volume sits outside the band, and is not positive while
   * every volume is inside it.
   */
  @Test
  void thresholdRangeReportsViolationOutsideBand() throws IOException {
    // 20% and 80% against a 50% ideal with a 10% threshold -> 20 points past the upper bound.
    List<DiskBalancerVolumeCalculation.VolumeFixedUsage> spread =
        Arrays.asList(usage("low", 100, 80), usage("high", 100, 20));
    DiskBalancerVolumeCalculation.ThresholdRange wideRange =
        DiskBalancerVolumeCalculation.ThresholdRange.of(spread, 10.0);

    assertEquals(0.2, wideRange.getViolation(spread), 0.01);
    assertFalse(wideRange.isWithinRange(spread));

    // Both volumes at 50% -> nothing outside the band.
    List<DiskBalancerVolumeCalculation.VolumeFixedUsage> even =
        Arrays.asList(usage("a", 100, 50), usage("b", 100, 50));
    DiskBalancerVolumeCalculation.ThresholdRange evenRange =
        DiskBalancerVolumeCalculation.ThresholdRange.of(even, 10.0);

    assertThat(evenRange.getViolation(even)).isLessThanOrEqualTo(0.0);
    assertTrue(evenRange.isWithinRange(even));
  }

  private DiskBalancerVolumeCalculation.VolumeFixedUsage usage(String name, long capacity,
      long available) throws IOException {
    return DiskBalancerVolumeCalculation.newVolumeFixedUsage(
        createVolume(name, capacity, available), null);
  }

  @Test
  void getIdealUsageReturnsZeroForZeroTotalCapacity() throws IOException {
    HddsVolume zeroCapacityVolume = createVolume("zero-capacity", 0, 0);

    assertEquals(0.0, DiskBalancerVolumeCalculation.getIdealUsage(
        Collections.singletonList(
            DiskBalancerVolumeCalculation.newVolumeFixedUsage(
                zeroCapacityVolume, null))));
  }

  @Test
  void calculateVolumeDataDensityIgnoresZeroCapacityVolumes()
      throws IOException {
    HddsVolume zeroCapacityVolume = createVolume("zero-capacity", 0, 0);
    HddsVolume lowUsageVolume = createVolume("low-usage", 100, 90);
    HddsVolume highUsageVolume = createVolume("high-usage", 100, 50);

    DiskBalancerVolumeCalculation.VolumeFixedUsage lowUsage =
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(lowUsageVolume, null);
    DiskBalancerVolumeCalculation.VolumeFixedUsage highUsage =
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(highUsageVolume, null);
    DiskBalancerVolumeCalculation.VolumeFixedUsage zeroCapacity =
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(
            zeroCapacityVolume, null);

    double densityWithoutZeroCapacityVolume =
        DiskBalancerVolumeCalculation.calculateVolumeDataDensity(
            Arrays.asList(lowUsage, highUsage));

    assertEquals(densityWithoutZeroCapacityVolume,
        DiskBalancerVolumeCalculation.calculateVolumeDataDensity(
            Arrays.asList(zeroCapacity, lowUsage, highUsage)), 0.0);
  }

  @Test
  void getUtilizationReturnsZeroForZeroCapacityVolume()
      throws IOException {
    HddsVolume volume = createVolume("zero-capacity-utilization", 0, 0);

    assertEquals(0.0, DiskBalancerVolumeCalculation.newVolumeFixedUsage(
        volume, null).getUtilization());
  }

  @Test
  void buildVolumeReportProtoIncludesStorageTypeForZeroCapacityVolume()
      throws IOException {
    HddsVolume volume = createVolume("zero-capacity-report", 0, 0, StorageType.SSD);

    VolumeReportProto report = DiskBalancerService.buildVolumeReportProto(
        Collections.singletonList(
            DiskBalancerVolumeCalculation.newVolumeFixedUsage(volume, null))).get(0);

    assertEquals(StorageTypeProto.SSD, report.getStorageType());
    assertEquals(0.0, report.getUtilization());
  }

  @Test
  void buildStorageTypeInfoCalculatesEachTypeIndependently() throws IOException {
    List<DiskBalancerVolumeCalculation.VolumeFixedUsage> volumeUsages = Arrays.asList(
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(
            createVolume("ssd-1", 100, 20, StorageType.SSD), null),
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(
            createVolume("ssd-2", 100, 20, StorageType.SSD), null),
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(
            createVolume("disk-1", 100, 80, StorageType.DISK), null),
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(
            createVolume("disk-2", 100, 80, StorageType.DISK), null));

    List<StorageTypeDiskBalancerInfoProto> result =
        DiskBalancerService.buildStorageTypeInfo(volumeUsages, 10.0, true);

    assertEquals(2, result.size());
    assertStorageTypeInfo(result.get(0), StorageTypeProto.SSD, 0.8);
    assertStorageTypeInfo(result.get(1), StorageTypeProto.DISK, 0.2);
  }

  @Test
  void buildStorageTypeInfoMarksSingleVolumeTypeNotBalanceable() throws IOException {
    List<DiskBalancerVolumeCalculation.VolumeFixedUsage> volumeUsages = Collections.singletonList(
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(
            createVolume("ssd", 100, 5, StorageType.SSD), null));

    StorageTypeDiskBalancerInfoProto result =
        DiskBalancerService.buildStorageTypeInfo(volumeUsages, 10.0, true).get(0);

    assertEquals(StorageTypeProto.SSD, result.getStorageType());
    assertEquals(1, result.getUsableVolumeCount());
    assertFalse(result.getBalanceable());
    assertFalse(result.hasIdealUsage());
    assertEquals(0, result.getBytesToMove());
    assertEquals(0.0, result.getCurrentVolumeDensitySum());
  }

  @Test
  void getIdealUsageRejectsNegativeCapacity() throws IOException {
    HddsVolume negativeCapacityVolume = createVolume(
        "negative-capacity", -1, 0);

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> DiskBalancerVolumeCalculation.getIdealUsage(
            Collections.singletonList(
                DiskBalancerVolumeCalculation.newVolumeFixedUsage(
                    negativeCapacityVolume, null))));

    assertEquals("Negative capacity = -1: " + negativeCapacityVolume,
        exception.getMessage());
  }

  @Test
  void getIdealUsageRejectsNegativeEffectiveUsed() throws IOException {
    HddsVolume volume = createVolume("negative-effective-used", 100, 100);
    DiskBalancerVolumeCalculation.VolumeFixedUsage volumeUsage =
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(
            volume, Collections.singletonMap(volume, -1L));

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> DiskBalancerVolumeCalculation.getIdealUsage(
            Collections.singletonList(volumeUsage)));

    assertEquals("Negative effective used = " + volumeUsage.getEffectiveUsed()
        + ": " + volume, exception.getMessage());
  }

  @Test
  void getIdealUsageRejectsEffectiveUsedGreaterThanCapacity()
      throws IOException {
    HddsVolume volume = createVolume("effective-used-exceeds-capacity", 100, 0);
    DiskBalancerVolumeCalculation.VolumeFixedUsage volumeUsage =
        DiskBalancerVolumeCalculation.newVolumeFixedUsage(
            volume, Collections.singletonMap(volume, 1L));

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> DiskBalancerVolumeCalculation.getIdealUsage(
            Collections.singletonList(volumeUsage)));

    assertEquals("Effective used = " + volumeUsage.getEffectiveUsed()
        + " > capacity = " + volumeUsage.getUsage().getCapacity() + ": "
        + volume, exception.getMessage());
  }

  private HddsVolume createVolume(String name, long capacity, long available)
      throws IOException {
    return createVolume(name, capacity, available, StorageType.DEFAULT);
  }

  private HddsVolume createVolume(String name, long capacity, long available,
      StorageType storageType) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    SpaceUsageSource source = MockSpaceUsageSource.fixed(capacity, available);
    SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
        source, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);

    return new HddsVolume.Builder(tempDir.resolve(name).toString())
        .conf(conf)
        .usageCheckFactory(factory)
        .storageType(storageType)
        .build();
  }

  private static void assertStorageTypeInfo(StorageTypeDiskBalancerInfoProto info,
      StorageTypeProto expectedType, double expectedIdealUsage) {
    assertEquals(expectedType, info.getStorageType());
    assertEquals(2, info.getUsableVolumeCount());
    assertTrue(info.getBalanceable());
    assertEquals(expectedIdealUsage, info.getIdealUsage(), 0.01);
    assertEquals(0, info.getBytesToMove());
    assertEquals(0.0, info.getCurrentVolumeDensitySum());
  }
}
