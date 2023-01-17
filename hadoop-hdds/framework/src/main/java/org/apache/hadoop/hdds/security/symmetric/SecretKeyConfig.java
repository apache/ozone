package org.apache.hadoop.hdds.security.symmetric;

import org.apache.hadoop.hdds.conf.ConfigurationSource;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_KEY_DIR_NAME_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ALGORITHM;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ALGORITHM_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_EXPIRY_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_EXPIRY_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_FILE;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_FILE_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SECRET_KEY_ROTATE_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;

public class SecretKeyConfig {
  private final Path localSecretKeyFile;
  private final Duration rotateDuration;
  private final Duration expiryDuration;
  private final String algorithm;

  public SecretKeyConfig(ConfigurationSource conf, String component) {
    String metadataDir = conf.get(HDDS_METADATA_DIR_NAME,
        conf.get(OZONE_METADATA_DIRS));
    String keyDir = conf.get(HDDS_KEY_DIR_NAME, HDDS_KEY_DIR_NAME_DEFAULT);
    String fileName = conf.get(HDDS_SECRET_KEY_FILE,
        HDDS_SECRET_KEY_FILE_DEFAULT);
    localSecretKeyFile = Paths.get(metadataDir, component, keyDir, fileName);

    String rotateDuration = conf.get(HDDS_SECRET_KEY_ROTATE_DURATION,
        HDDS_SECRET_KEY_ROTATE_DURATION_DEFAULT);
    this.rotateDuration = Duration.parse(rotateDuration);

    String expiryDuration = conf.get(HDDS_SECRET_KEY_EXPIRY_DURATION,
        HDDS_SECRET_KEY_EXPIRY_DURATION_DEFAULT);
    this.expiryDuration = Duration.parse(expiryDuration);

    this.algorithm = conf.get(HDDS_SECRET_KEY_ALGORITHM,
        HDDS_SECRET_KEY_ALGORITHM_DEFAULT);
  }

  public Path getLocalSecretKeyFile() {
    return localSecretKeyFile;
  }

  public Duration getRotateDuration() {
    return rotateDuration;
  }

  public Duration getExpiryDuration() {
    return expiryDuration;
  }

  public String getAlgorithm() {
    return algorithm;
  }
}
