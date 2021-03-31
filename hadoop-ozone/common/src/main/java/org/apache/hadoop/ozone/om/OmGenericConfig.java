package org.apache.hadoop.ozone.om;

import java.time.Duration;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

@ConfigGroup(prefix = "ozone.om")
public class OmGenericConfig {

  @Config(key = "upgrade.finalization.ratis.based.timeout",
      defaultValue = "30s",
      type = ConfigType.TIME,
      tags = {ConfigTag.OM, ConfigTag.UPGRADE},
      description = "Maximum time to wait for a slow follower to be finalized" +
          " through a Ratis snapshot. This is an advanced config, and needs " +
          "to be changed only under a special circumstance when the leader OM" +
          " has purged the finalize request from its logs, and a follower OM " +
          "was down during upgrade finalization. Default is 30s."
  )
  private long ratisBasedFinalizationTimeout =
      Duration.ofSeconds(30).getSeconds();

  @Config(key = "init.default.layout.version",
      defaultValue = "-1",
      type = ConfigType.INT,
      tags = {ConfigTag.OM, ConfigTag.UPGRADE},
      description = "Default Layout Version to init the OM with. Intended to "
          + "be used in tests to finalize from an older version of OM to the "
          + "latest. By default, OM init uses the highest layout version."
  )
  private int defaultLayoutVersionOnInit = -1;

  public static class ConfigStrings {
    public static final String OZONE_OM_INIT_DEFAULT_LAYOUT_VERSION =
        "ozone.om.init.default.layout.version";
  }

  public long getRatisBasedFinalizationTimeout() {
    return ratisBasedFinalizationTimeout;
  }

  public void setRatisBasedFinalizationTimeout(long timeout) {
    this.ratisBasedFinalizationTimeout = timeout;
  }

  public int getDefaultLayoutVersionOnInit() {
    return defaultLayoutVersionOnInit;
  }

  public void setDefaultLayoutVersionOnInit(int defaultLayoutVersionOnInit) {
    this.defaultLayoutVersionOnInit = defaultLayoutVersionOnInit;
  }


}
