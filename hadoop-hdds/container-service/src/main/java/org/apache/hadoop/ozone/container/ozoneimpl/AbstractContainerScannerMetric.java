package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

/**
 * Base class for container scanner metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "Datanode container scanner metrics", context = "dfs")
public class AbstractContainerScannerMetric {

  private final String name;
  private final MetricsSystem ms;

  @Metric("number of containers scanned in the current iteration")
  private MutableGaugeInt numContainersScanned;
  @Metric("number of unhealthy containers found in the current iteration")
  private MutableGaugeInt numUnHealthyContainers;
  @Metric("number of iterations of scanner completed since the restart")
  private MutableCounterInt numScanIterations;

  public AbstractContainerScannerMetric(String name, MetricsSystem ms) {
    this.name = name;
    this.ms = ms;
  }

  public int getNumContainersScanned() {
    return numContainersScanned.value();
  }

  public void incNumContainersScanned() {
    numContainersScanned.incr();
  }

  public void resetNumContainersScanned() {
    numContainersScanned.decr(getNumContainersScanned());
  }

  public int getNumUnHealthyContainers() {
    return numUnHealthyContainers.value();
  }

  public void incNumUnHealthyContainers() {
    numUnHealthyContainers.incr();
  }

  public void resetNumUnhealthyContainers() {
    numUnHealthyContainers.decr(getNumUnHealthyContainers());
  }

  public int getNumScanIterations() {
    return numScanIterations.value();
  }

  public void incNumScanIterations() {
    numScanIterations.incr();
  }

  public void unregister() {
    ms.unregisterSource(name);
  }

  public String getName() {
    return name;
  }
}
