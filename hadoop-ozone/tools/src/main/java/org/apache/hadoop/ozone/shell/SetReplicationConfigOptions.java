package org.apache.hadoop.ozone.shell;

import picocli.CommandLine;

/**
 * Common options for set replication config.
 */
public class SetReplicationConfigOptions {
  @CommandLine.Option(names = {"--replication",
      "-r"}, description = "Replication value. Example: 3 (for Ratis type)"
      + " or 1 ( for" + " standalone type). In the case of EC, pass"
      + " DATA-PARITY, eg 3-2," + " 6-3, 10-4")
  private String replication;

  @CommandLine.Option(names = {"--type",
      "-t"}, description = "Replication type. Supported types are RATIS,"
      + " STAND_ALONE, EC")
  private String replicationType;

  public String getReplication() {
    return this.replication;
  }

  public String getType() {
    return this.replicationType;
  }
}
