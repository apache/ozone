package org.apache.hadoop.hdds.scm.upgrade;

import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManagerImpl;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;

import java.io.IOException;

/**
 * An implementation of {@link FinalizationManager} that supports injecting
 * the {@link FinalizationStateManager} for testing.
 */
public class FinalizationManagerTestImpl extends FinalizationManagerImpl {

  public FinalizationManagerTestImpl(Builder builder) throws IOException {
    super(builder, builder.finalizationStateManager);
  }

  /**
   * Builds a {@link FinalizationManagerTestImpl}.
   */
  public static class Builder extends FinalizationManagerImpl.Builder {
    private FinalizationStateManager finalizationStateManager;

    public Builder setFinalizationStateManager(
        FinalizationStateManager stateManager) {
      this.finalizationStateManager = stateManager;
      return this;
    }

    @Override
    public FinalizationManagerTestImpl build() throws IOException {
      return new FinalizationManagerTestImpl(this);
    }
  }
}
