package org.apache.hadoop.ozone.web.ozShell.s3;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.web.ozShell.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Common interface for S3 command handling.
 */
@CommandLine.Command(mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class S3Handler extends Handler {
  protected static final Logger LOG = LoggerFactory.getLogger(S3Handler.class);

  @CommandLine.Option(names = {"--om-service-id"},
      required = false,
      description = "OM Service ID is required to be specified for OM HA" +
          " cluster")
  private String omServiceID;

  public String getOmServiceID() {
    return omServiceID;
  }
}
