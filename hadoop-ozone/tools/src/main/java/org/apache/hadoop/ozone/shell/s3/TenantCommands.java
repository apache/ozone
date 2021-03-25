package org.apache.hadoop.ozone.shell.s3;

import org.apache.hadoop.hdds.cli.GenericParentCommand;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.cli.MissingSubcommandException;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.shell.OzoneShell;
import org.apache.hadoop.ozone.shell.Shell;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * Subcommand to group tenant related operations.
 */
// TODO: See VolumeCommands
@CommandLine.Command(name = "tenant",
    description = "Tenant management operations",
    subcommands = {
        TenantCreateHandler.class
    },
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
@MetaInfServices(SubcommandWithParent.class)
public class TenantCommands implements GenericParentCommand, Callable<Void>,
    SubcommandWithParent {

  @CommandLine.ParentCommand
  private Shell shell;

  @Override
  public Void call() throws Exception {
    throw new MissingSubcommandException(
        this.shell.getCmd().getSubcommands().get("tenant"));
  }

  @Override
  public boolean isVerbose() {
    return shell.isVerbose();
  }

  @Override
  public OzoneConfiguration createOzoneConfiguration() {
    return shell.createOzoneConfiguration();
  }

  @Override
  public Class<?> getParentType() {
    return OzoneShell.class;
  }
}
