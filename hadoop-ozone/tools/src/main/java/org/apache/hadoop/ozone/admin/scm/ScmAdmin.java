package org.apache.hadoop.ozone.admin.scm;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(
    name = "scm",
    description = "Ozone Storage Container Manager specific admin operations",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        GetScmRatisStatusSubcommand.class
    })
public class ScmAdmin extends GenericCli {

  @CommandLine.ParentCommand
  private OzoneAdmin parent;

  @Spec
  private CommandSpec spec;

  public OzoneAdmin getParent() {
    return parent;
  }

  @Override
  public Void call() throws Exception {
    GenericCli.missingSubcommand(spec);
    return null;
  }

  public ScmClient createScmClient() {
    return parent.createScmClient();
  }
}
