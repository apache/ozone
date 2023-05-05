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

@RunWith(Parameterized.class)
public class TestBackgroundContainerDataScanner extends TestContainerScannersAbstract {

  private final ContainerCorruption corruption;

  @Parameterized.Parameters
  public static Collection<Object[]> supportedCorruptionTypes() {
    return Arrays.asList(new Object[][] {
        { MISSING_CHUNKS_DIR },
        { MISSING_METADATA_DIR },
        { MISSING_CONTAINER_DIR },
        { MISSING_CONTAINER_FILE },
        { CORRUPT_CONTAINER_FILE },
    });
  }

  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set(ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_ENABLED,
        String.valueOf(true));
    buildCluster(ozoneConfig);
  }

  public TestBackgroundContainerDataScanner(ContainerCorruption corruption) {
    this.corruption = corruption;
  }

  /**
   * {@link BackgroundContainerDataScanner} should detect corrupted blocks
   * in a closed container without client interaction.
   */
  @Test
  public void testCorruptionDetected() throws Exception {
    long containerID = writeDataThenCloseContainer();
    corruption.applyTo(getContainer(containerID));

    // Container corruption is not yet detected.
    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
        getContainer(containerID).getContainerState());
    // Wait for the scanner to detect corruption.
    GenericTestUtils.waitFor(() ->
            getContainer(containerID).getContainerState() ==
                ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        1000, (int)SCAN_INTERVAL.toMillis() * 2);

    // Wait for SCM to get a report of the unhealthy replica.
    waitForScmToSeeUnhealthy(containerID);
  }
}
