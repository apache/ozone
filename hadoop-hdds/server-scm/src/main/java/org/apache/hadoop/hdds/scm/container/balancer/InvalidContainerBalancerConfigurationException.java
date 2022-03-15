package org.apache.hadoop.hdds.scm.container.balancer;

/**
 * Signals that {@link ContainerBalancerConfiguration} contains invalid
 * configuration value(s).
 */
public class InvalidContainerBalancerConfigurationException extends Exception {

  /**
   * Constructs an InvalidContainerBalancerConfigurationException with no detail
   * message. A detail message is a String that describes this particular
   * exception.
   */
  public InvalidContainerBalancerConfigurationException() {
    super();
  }

  /**
   * Constructs an InvalidContainerBalancerConfigurationException with the
   * specified detail message. A detail message is a String that describes
   * this particular exception.
   *
   * @param s the String that contains a detailed message
   */
  public InvalidContainerBalancerConfigurationException(String s) {
    super(s);
  }

}
