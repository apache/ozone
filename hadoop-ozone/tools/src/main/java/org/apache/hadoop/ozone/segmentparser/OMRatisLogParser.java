package org.apache.hadoop.ozone.segmentparser;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Command line utility to parse and dump a OM ratis segment file.
 */
@CommandLine.Command(
    name = "om",
    description = "dump om ratis segment file",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class OMRatisLogParser extends BaseLogParser implements Callable<Void> {

  @Override
  public Void call() throws Exception {
    System.out.println("Dumping OM Ratis Log");

    parseRatisLogs(OMRatisHelper::smProtoToString);
    return null;
  }
}

