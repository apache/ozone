package org.apache.hadoop.ozone.admin.ratis.group;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.ozone.admin.ratis.RatisAdmin;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

/**
 * Subcommand for ratis.
 */
@CommandLine.Command(
    name = "group",
    description = "ratis group operations",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class,
    subcommands = {
        InfoCommand.class,
        ListCommand.class
    })
@MetaInfServices(SubcommandWithParent.class)
public class GroupCommand extends GenericCli implements SubcommandWithParent {

  @CommandLine.ParentCommand
  private RatisAdmin parent;

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  public RatisAdmin getParent() {
    return parent;
  }

  @Override
  public Void call() throws Exception {
    GenericCli.missingSubcommand(spec);
    return null;
  }

  @Override
  public Class<?> getParentType() {
    return RatisAdmin.class;
  }


}
