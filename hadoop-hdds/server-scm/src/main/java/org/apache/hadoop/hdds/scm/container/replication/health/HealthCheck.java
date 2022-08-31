package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;

public interface HealthCheck {

  /**
   * Handle the container and apply the given check to it. This may result in
   * commands getting generated to correct container states, or nothing
   * happening if the container is healthy.
   * @param request ContainerCheckRequest object representing the container
   * @return True if the request was handled or false if it was not and should
   *         be handled by the next handler in the chain.
   */
  boolean handle(ContainerCheckRequest request);

  void addNext(HealthCheck handler);

}
