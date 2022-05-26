package org.apache.hadoop.hdds.scm.upgrade;

import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManagerImpl;

import java.io.IOException;

/**
 * An implementation of {@link FinalizationStateManager} built without an
 * invocation handler for testing.
 */
public class FinalizationStateManagerTestImpl extends FinalizationStateManagerImpl {
  public FinalizationStateManagerTestImpl(Builder builder) throws IOException {
    super(builder);
  }

  public static class Builder extends FinalizationStateManagerImpl.Builder {
    @Override
    public FinalizationStateManagerTestImpl build() throws IOException {
      return new FinalizationStateManagerTestImpl(this);
    }
  }
}
