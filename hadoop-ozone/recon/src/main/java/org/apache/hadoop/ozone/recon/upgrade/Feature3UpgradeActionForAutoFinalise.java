package org.apache.hadoop.ozone.recon.upgrade;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.recon.upgrade.ReconLayoutFeature.FEATURE_3;
import static org.apache.hadoop.ozone.recon.upgrade.ReconUpgradeAction.UpgradeActionType.AUTO_FINALIZE;
import static org.apache.hadoop.ozone.recon.upgrade.ReconUpgradeAction.UpgradeActionType.TEST_ACTION;

@UpgradeActionRecon(feature = FEATURE_3, type = AUTO_FINALIZE)
public class Feature3UpgradeActionForAutoFinalise implements ReconUpgradeAction {

  private static final Logger LOG = LoggerFactory.getLogger(
      Feature3UpgradeActionForAutoFinalise.class);

  @Override
  public void execute() throws Exception {
    // Logic for Feature 3 upgrade
    LOG.info("Executing Feature 3 upgrade script for action type AUTO_FINALIZE...");
  }

  @Override
  public UpgradeActionType getType() {
    return AUTO_FINALIZE;
  }
}