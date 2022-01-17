


package org.apache.hadoop.hdds.cli;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.NativeCodeLoader;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import picocli.CommandLine;

import java.util.function.Supplier;

/**
 * Ozone Admin Command line tool.
 */
@CommandLine.Command(name = "ozone admin",
    hidden = true,
    description = "Developer tools for Ozone Admin operations",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class OzoneAdmin extends GenericCli {

  private OzoneConfiguration ozoneConf;

  private UserGroupInformation user;

  public OzoneAdmin() {
    super(OzoneAdmin.class);
  }

  @VisibleForTesting
  public OzoneAdmin(OzoneConfiguration conf) {
    super(OzoneAdmin.class);
    ozoneConf = conf;
  }

  public OzoneConfiguration getOzoneConf() {
    if (ozoneConf == null) {
      ozoneConf = createOzoneConfiguration();
    }
    return ozoneConf;
  }

  public UserGroupInformation getUser() throws IOException {
    if (user == null) {
      user = UserGroupInformation.getCurrentUser();
    }
    return user;
  }

  /**
   * Main for the Ozone Admin shell Command handling.
   *
   * @param argv - System Args Strings[]
   */
  public static void main(String[] argv) {
    LogManager.resetConfiguration();
    Logger.getRootLogger().setLevel(Level.INFO);
    Logger.getRootLogger()
        .addAppender(new ConsoleAppender(new PatternLayout("%m%n")));
    Logger.getLogger(NativeCodeLoader.class).setLevel(Level.ERROR);

    new OzoneAdmin().run(argv);
  }

  @Override
  public void execute(String[] argv) {
    TracingUtil.initTracing("shell", createOzoneConfiguration());
    TracingUtil.executeInNewSpan("main",
        (Supplier<Void>) () -> {
          super.execute(argv);
          return null;
        });
  }
}
