package org.apache.hadoop.ozone.recon;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.apache.hadoop.ozone.recon.upgrade.InitialConstraintUpgradeAction;

/**
 * Guice module for binding upgrade actions as singletons in Recon.
 */
public class ReconUpgradeActionsModule extends AbstractModule {

  @Override
  protected void configure() {
    // Bind UpdateUnhealthyContainersConstraintAction as a singleton
    bind(InitialConstraintUpgradeAction.class).in(Singleton.class);
  }
}