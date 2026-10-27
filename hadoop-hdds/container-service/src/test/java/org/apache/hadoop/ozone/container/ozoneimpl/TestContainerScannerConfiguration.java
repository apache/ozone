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

package org.apache.hadoop.ozone.container.ozoneimpl;

import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.BANDWIDTH_PER_VOLUME_DEFAULT;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.CONTAINER_SCAN_MIN_GAP;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.CONTAINER_SCAN_MIN_GAP_DEFAULT;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.DATA_SCAN_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.DATA_SCAN_INTERVAL_KEY;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.METADATA_SCAN_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.METADATA_SCAN_INTERVAL_KEY;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.ON_DEMAND_BANDWIDTH_PER_VOLUME_DEFAULT;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.ON_DEMAND_VOLUME_BYTES_PER_SECOND_KEY;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.VOLUME_BYTES_PER_SECOND_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link ContainerScannerConfiguration}.
 */
public class TestContainerScannerConfiguration {

  private OzoneConfiguration conf;

  @BeforeEach
  public void setup() {
    this.conf = new OzoneConfiguration();
  }

  @Test
  public void acceptsValidValues() {
    long validInterval = Duration.ofHours(1).toMillis();
    long validBandwidth = (long) StorageUnit.MB.toBytes(1);
    long validOnDemandBandwidth = (long) StorageUnit.MB.toBytes(2);

    conf.setLong(METADATA_SCAN_INTERVAL_KEY, validInterval);
    conf.setLong(DATA_SCAN_INTERVAL_KEY, validInterval);
    conf.setLong(CONTAINER_SCAN_MIN_GAP, validInterval);
    conf.setLong(VOLUME_BYTES_PER_SECOND_KEY, validBandwidth);
    conf.setLong(ON_DEMAND_VOLUME_BYTES_PER_SECOND_KEY, validOnDemandBandwidth);

    ContainerScannerConfiguration csConf =
        conf.getObject(ContainerScannerConfiguration.class);

    assertEquals(validInterval, csConf.getMetadataScanInterval());
    assertEquals(validInterval, csConf.getDataScanInterval());
    assertEquals(validInterval, csConf.getContainerScanMinGap());
    assertEquals(validBandwidth, csConf.getBandwidthPerVolume());
    assertEquals(validOnDemandBandwidth,
        csConf.getOnDemandBandwidthPerVolume());
  }

  @Test
  public void overridesInvalidValues() {
    long invalidInterval = -1;
    long invalidBandwidth = -1;

    conf.setLong(METADATA_SCAN_INTERVAL_KEY, invalidInterval);
    conf.setLong(DATA_SCAN_INTERVAL_KEY, invalidInterval);
    conf.setLong(CONTAINER_SCAN_MIN_GAP, invalidInterval);
    conf.setLong(VOLUME_BYTES_PER_SECOND_KEY, invalidBandwidth);
    conf.setLong(ON_DEMAND_VOLUME_BYTES_PER_SECOND_KEY, invalidBandwidth);

    ContainerScannerConfiguration csConf =
        conf.getObject(ContainerScannerConfiguration.class);

    assertEquals(METADATA_SCAN_INTERVAL_DEFAULT,
        csConf.getMetadataScanInterval());
    assertEquals(DATA_SCAN_INTERVAL_DEFAULT,
        csConf.getDataScanInterval());
    assertEquals(CONTAINER_SCAN_MIN_GAP_DEFAULT,
        csConf.getContainerScanMinGap());
    assertEquals(BANDWIDTH_PER_VOLUME_DEFAULT,
        csConf.getBandwidthPerVolume());
    assertEquals(ON_DEMAND_BANDWIDTH_PER_VOLUME_DEFAULT,
        csConf.getOnDemandBandwidthPerVolume());
  }

  @Test
  public void isCreatedWitDefaultValues() {
    ContainerScannerConfiguration csConf =
        conf.getObject(ContainerScannerConfiguration.class);

    assertTrue(csConf.isEnabled());
    assertEquals(METADATA_SCAN_INTERVAL_DEFAULT,
        csConf.getMetadataScanInterval());
    assertEquals(DATA_SCAN_INTERVAL_DEFAULT,
        csConf.getDataScanInterval());
    assertEquals(BANDWIDTH_PER_VOLUME_DEFAULT,
        csConf.getBandwidthPerVolume());
    assertEquals(ON_DEMAND_BANDWIDTH_PER_VOLUME_DEFAULT,
        csConf.getOnDemandBandwidthPerVolume());
    assertEquals(CONTAINER_SCAN_MIN_GAP_DEFAULT,
        csConf.getContainerScanMinGap());
  }
}
