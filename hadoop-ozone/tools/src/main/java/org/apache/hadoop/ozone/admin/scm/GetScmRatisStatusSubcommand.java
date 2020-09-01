package org.apache.hadoop.ozone.admin.scm;

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

@CommandLine.Command(
    name = "status",
    description = "",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class GetScmRatisStatusSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private ScmAdmin parent;

  @Override
  public Void call() throws Exception {
    ScmClient scmClient = parent.createScmClient();
    List<String> status = scmClient.getScmRatisStatus();
    System.out.println(status);
    return null;
  }
}
