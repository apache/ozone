package org.apache.hadoop.ozone.recon.upgrade;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.recon.upgrade.ReconLayoutFeature.FEATURE_1;
import static org.apache.hadoop.ozone.recon.upgrade.ReconUpgradeAction.UpgradeActionType.AUTO_FINALIZE;

@UpgradeActionRecon(feature = FEATURE_1, type = AUTO_FINALIZE)
public class Feature1UpgradeActionForAutoFinalise implements ReconUpgradeAction {

  private static final Logger LOG = LoggerFactory.getLogger(
      Feature1UpgradeActionForAutoFinalise.class);

  @Override
  public void execute() throws Exception {
    // Logic for Feature 1 upgrade
    LOG.info("Executing Feature 1 upgrade script for action type AUTO_FINALIZE...");
  }

  @Override
  public UpgradeActionType getType() {
    return AUTO_FINALIZE;
  }
}
