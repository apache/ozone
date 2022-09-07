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

  /**
   * Starting from this HealthCheck, call the handle method. If it returns
   * false, indicating the request was not handled, then forward the request to
   * the next handler in the chain via its handleChain method. Repeating until
   * the request is handled, or there are no further handlers to try.
   * @param request
   * @return True if the request was handled or false if not handler handled it.
   */
  boolean handleChain(ContainerCheckRequest request);

  HealthCheck addNext(HealthCheck handler);

}
