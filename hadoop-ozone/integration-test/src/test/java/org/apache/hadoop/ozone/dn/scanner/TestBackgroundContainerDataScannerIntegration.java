package org.apache.hadoop.ozone.dn.scanner;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.ozoneimpl.BackgroundContainerDataScanner;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for the background container data scanner. This scanner
 * checks all data and metadata in the container.
 */
@RunWith(Parameterized.class)
public class TestBackgroundContainerDataScannerIntegration
    extends TestContainerScannerIntegrationAbstract {

  private final ContainerCorruption corruption;

  @Parameterized.Parameters
  public static Collection<Object[]> supportedCorruptionTypes() {
    return Arrays.asList(new Object[][] {
        {MISSING_CHUNKS_DIR},
        {MISSING_METADATA_DIR},
        {MISSING_CONTAINER_DIR},
        {MISSING_CONTAINER_FILE},
        {CORRUPT_CONTAINER_FILE},
        {CORRUPT_BLOCK},
        {MISSING_BLOCK},
    });
  }

  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_ENABLED, true);
    // Make sure the background metadata scanner does not detect failures
    // before the data scanner under test does.
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_DEV_METADATA_ENABLED,
        false);
    // Make the background data scanner run frequently to reduce test time.
    ozoneConfig.setTimeDuration(
        ContainerScannerConfiguration.DATA_SCAN_INTERVAL_KEY,
        SCAN_INTERVAL.getSeconds(), TimeUnit.SECONDS);
    buildCluster(ozoneConfig);
  }

  public TestBackgroundContainerDataScannerIntegration(
      ContainerCorruption corruption) {
    this.corruption = corruption;
  }

  /**
   * {@link BackgroundContainerDataScanner} should detect corrupted blocks
   * in a closed container without client interaction.
   */
  @Test
  public void testCorruptionDetected() throws Exception {
    long containerID = writeDataThenCloseContainer();
    // Container corruption has not yet been introduced.
    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
        getContainer(containerID).getContainerState());

    corruption.applyTo(getContainer(containerID));
    // Wait for the scanner to detect corruption.
    GenericTestUtils.waitFor(() ->
            getContainer(containerID).getContainerState() ==
                ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        1000, (int)SCAN_INTERVAL.toMillis() * 2);

    // Wait for SCM to get a report of the unhealthy replica.
    waitForScmToSeeUnhealthy(containerID);
  }
}
