package org.apache.hadoop.ozone.om.upgrade;

import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;

import java.io.IOException;

public class OMUpgradeFinalizer implements UpgradeFinalizer {

  private Status status = Status.ALREADY_FINALIZED;
  private OmLayoutVersionManager versionManager;

  public OMUpgradeFinalizer(OmLayoutVersionManager versionManager) {
    this.versionManager = versionManager;
    if (versionManager.needsFinalization()) {
      status = Status.FINALIZATION_REQUIRED;
    }
  }

  @Override
  public StatusAndMessages finalize(String clientID) throws IOException {
    if (!versionManager.needsFinalization()) {
      return FINALIZED_MSG;
    }
    return null;
  }

  @Override
  public StatusAndMessages reportStatus() throws IOException {
    return null;
  }
}
