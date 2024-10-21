package org.apache.hadoop.ozone.recon.upgrade;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.recon.upgrade.ReconLayoutFeature.FEATURE_3;
import static org.apache.hadoop.ozone.recon.upgrade.ReconUpgradeAction.UpgradeActionType.TEST_ACTION;

@UpgradeActionRecon(feature = FEATURE_3, type = TEST_ACTION)
public class Feature3UpgradeActionForTestAction implements ReconUpgradeAction {
  private static final Logger LOG = LoggerFactory.getLogger(
      Feature3UpgradeActionForTestAction.class);

  @Override
  public void execute() throws Exception {
    LOG.info("Executing Feature 3 upgrade script for action type TEST_ACTION...");
  }

  @Override
  public UpgradeActionType getType() {
    return TEST_ACTION;
  }
}
