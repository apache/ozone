package org.apache.hadoop.ozone.conf;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOzoneConfigUtils {

  @Test
  public void testS3AdminExtraction() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_S3_ADMINISTRATORS, "alice,bob");

    assertThat(OzoneS3ConfigUtils.getS3AdminsFromConfig(configuration))
        .containsAll(Arrays.asList("alice", "bob"));
  }

  @Test
  public void testS3AdminExtractionWithFallback() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_ADMINISTRATORS, "alice,bob");

    assertThat(OzoneS3ConfigUtils.getS3AdminsFromConfig(configuration))
        .containsAll(Arrays.asList("alice", "bob"));
  }

  @Test
  public void testS3AdminGroupExtraction() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_S3_ADMINISTRATORS_GROUPS,
        "test1, test2");

    assertThat(OzoneS3ConfigUtils.getS3AdminsGroupsFromConfig(configuration))
        .containsAll(Arrays.asList("test1", "test2"));
  }

  @Test
  public void testS3AdminGroupExtractionWithFallback() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS,
        "test1, test2");

    assertThat(OzoneS3ConfigUtils.getS3AdminsGroupsFromConfig(configuration))
        .containsAll(Arrays.asList("test1", "test2"));
  }
}
