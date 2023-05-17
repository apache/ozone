package org.apache.hadoop.ozone.dn.scanner;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.ozoneimpl.BackgroundContainerMetadataScanner;
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
 * Integration tests for the background container metadata scanner. This
 * scanner does a quick check of container metadata to find obvious failures
 * faster than a full data scan.
 */
@RunWith(Parameterized.class)
public class TestBackgroundContainerMetadataScannerIntegration
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
    });
  }

  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_ENABLED, true);
    // Make sure the background data scanner does not detect failures
    // before the data scanner under test does.
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_DEV_DATA_ENABLED,
        false);
    // Make the background metadata scanner run frequently to reduce test time.
    ozoneConfig.setTimeDuration(
        ContainerScannerConfiguration.METADATA_SCAN_INTERVAL_KEY,
        SCAN_INTERVAL.getSeconds(), TimeUnit.SECONDS);
    buildCluster(ozoneConfig);
  }

  public TestBackgroundContainerMetadataScannerIntegration(
      ContainerCorruption corruption) {
    this.corruption = corruption;
  }

  /**
   * {@link BackgroundContainerMetadataScanner} should detect corrupted metadata
   * in open or closed containers without client interaction.
   */
  @Test
  public void testCorruptionDetected() throws Exception {
    // Write data to an open and closed container.
    long closedContainerID = writeDataThenCloseContainer();
    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
        getContainer(closedContainerID).getContainerState());
    long openContainerID = writeDataToOpenContainer();
    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.OPEN,
        getContainer(openContainerID).getContainerState());

    // Corrupt both containers.
    corruption.applyTo(getContainer(closedContainerID));
    corruption.applyTo(getContainer(openContainerID));
    // Wait for the scanner to detect corruption.
    GenericTestUtils.waitFor(() ->
            getContainer(closedContainerID).getContainerState() ==
                ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        1000, (int)SCAN_INTERVAL.toMillis() * 2);
    GenericTestUtils.waitFor(() ->
            getContainer(openContainerID).getContainerState() ==
                ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        1000, (int)SCAN_INTERVAL.toMillis() * 2);

    // Wait for SCM to get reports of the unhealthy replicas.
    waitForScmToSeeUnhealthy(closedContainerID);
    waitForScmToSeeUnhealthy(openContainerID);
  }
}
