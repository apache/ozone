package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;

public abstract class AbstractCheck implements HealthCheck {

  private HealthCheck successor = null;

  public boolean handleChain(ContainerCheckRequest request) {
    boolean result = handle(request);
    if (!result && successor != null) {
      return successor.handleChain(request);
    }
    return result;
  }

  @Override
  public HealthCheck addNext(HealthCheck healthCheck) {
    successor = healthCheck;
    return healthCheck;
  }

  @Override
  public abstract boolean handle(ContainerCheckRequest request);

}
