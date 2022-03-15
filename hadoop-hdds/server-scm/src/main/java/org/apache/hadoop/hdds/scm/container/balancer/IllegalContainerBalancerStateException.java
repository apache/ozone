package org.apache.hadoop.hdds.scm.container.balancer;

/**
 * Signals that a state change cannot be performed on ContainerBalancer.
 */
public class IllegalContainerBalancerStateException extends Exception {

  /**
   * Constructs an IllegalContainerBalancerStateException with no detail
   * message. A detail message is a String that describes this particular
   * exception.
   */
  public IllegalContainerBalancerStateException() {
    super();
  }

  /**
   * Constructs an IllegalContainerBalancerStateException with the specified
   * detail message. A detail message is a String that describes this particular
   * exception.
   *
   * @param s the String that contains a detailed message
   */
  public IllegalContainerBalancerStateException(String s) {
    super(s);
  }

}
