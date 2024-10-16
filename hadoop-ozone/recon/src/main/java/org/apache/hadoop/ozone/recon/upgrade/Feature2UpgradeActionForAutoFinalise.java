package org.apache.hadoop.ozone.recon.upgrade;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.recon.upgrade.ReconLayoutFeature.FEATURE_2;
import static org.apache.hadoop.ozone.recon.upgrade.ReconUpgradeAction.UpgradeActionType.AUTO_FINALIZE;
import static org.apache.hadoop.ozone.recon.upgrade.ReconUpgradeAction.UpgradeActionType.TEST_ACTION;

@UpgradeActionRecon(feature = FEATURE_2, type = AUTO_FINALIZE)
public class Feature2UpgradeActionForAutoFinalise implements ReconUpgradeAction {

  private static final Logger LOG = LoggerFactory.getLogger(
      Feature2UpgradeActionForAutoFinalise.class);

  @Override
  public void execute() throws Exception {
    // Logic for Feature 2 upgrade
    LOG.info("Executing Feature 2 upgrade script for action type AUTO_FINALIZE...");
  }

  @Override
  public UpgradeActionType getType() {
    return TEST_ACTION;
  }
}