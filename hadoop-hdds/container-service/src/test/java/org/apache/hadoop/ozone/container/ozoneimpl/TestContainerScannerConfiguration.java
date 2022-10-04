/*
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
package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.BANDWIDTH_PER_VOLUME_DEFAULT;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.DATA_SCAN_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.DATA_SCAN_INTERVAL_KEY;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.METADATA_SCAN_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.METADATA_SCAN_INTERVAL_KEY;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration.VOLUME_BYTES_PER_SECOND_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link ContainerScannerConfiguration}.
 */
public class TestContainerScannerConfiguration {

  private OzoneConfiguration conf;

  @Before
  public void setup() {
    this.conf = new OzoneConfiguration();
  }

  @Test
  public void acceptsValidValues() {
    long validInterval = Duration.ofHours(1).toMillis();
    long validBandwidth = (long) StorageUnit.MB.toBytes(1);

    conf.setLong(METADATA_SCAN_INTERVAL_KEY, validInterval);
    conf.setLong(DATA_SCAN_INTERVAL_KEY, validInterval);
    conf.setLong(VOLUME_BYTES_PER_SECOND_KEY, validBandwidth);

    ContainerScannerConfiguration csConf =
        conf.getObject(ContainerScannerConfiguration.class);

    assertEquals(validInterval, csConf.getMetadataScanInterval());
    assertEquals(validInterval, csConf.getDataScanInterval());
    assertEquals(validBandwidth, csConf.getBandwidthPerVolume());
  }

  @Test
  public void overridesInvalidValues() {
    long invalidInterval = -1;
    long invalidBandwidth = -1;

    conf.setLong(METADATA_SCAN_INTERVAL_KEY, invalidInterval);
    conf.setLong(DATA_SCAN_INTERVAL_KEY, invalidInterval);
    conf.setLong(VOLUME_BYTES_PER_SECOND_KEY, invalidBandwidth);

    ContainerScannerConfiguration csConf =
        conf.getObject(ContainerScannerConfiguration.class);

    assertEquals(METADATA_SCAN_INTERVAL_DEFAULT,
        csConf.getMetadataScanInterval());
    assertEquals(DATA_SCAN_INTERVAL_DEFAULT,
        csConf.getDataScanInterval());
    assertEquals(BANDWIDTH_PER_VOLUME_DEFAULT,
        csConf.getBandwidthPerVolume());
  }

  @Test
  public void isCreatedWitDefaultValues() {
    ContainerScannerConfiguration csConf =
        conf.getObject(ContainerScannerConfiguration.class);

    assertEquals(false, csConf.isEnabled());
    assertEquals(METADATA_SCAN_INTERVAL_DEFAULT,
        csConf.getMetadataScanInterval());
    assertEquals(DATA_SCAN_INTERVAL_DEFAULT,
        csConf.getDataScanInterval());
    assertEquals(BANDWIDTH_PER_VOLUME_DEFAULT,
        csConf.getBandwidthPerVolume());
  }
}
