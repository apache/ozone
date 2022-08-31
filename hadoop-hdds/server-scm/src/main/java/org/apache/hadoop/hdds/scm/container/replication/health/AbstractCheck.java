package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;

public abstract class AbstractCheck implements HealthCheck {

  private HealthCheck successor = null;

  public void handleChain(ContainerCheckRequest request) {
    boolean result = handle(request);
    if (!result && successor != null) {
      successor.handle(request);
    }
  }

  @Override
  public void addNext(HealthCheck healthCheck) {
    successor = healthCheck;
  }

  @Override
  public abstract boolean handle(ContainerCheckRequest request);

}
