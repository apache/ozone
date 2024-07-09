package org.apache.hadoop.hdds.scm.container.balancer;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Info about balancer status.
 */
public class ContainerBalancerStatusInfo {
  private final OffsetDateTime startedAt;
  private final ContainerBalancerConfiguration configuration;
  private final List<ContainerBalancerTaskIterationStatusInfo> iterationsStatusInfo;

  public ContainerBalancerStatusInfo(
      OffsetDateTime startedAt,
      ContainerBalancerConfiguration configuration,
      List<ContainerBalancerTaskIterationStatusInfo> iterationsStatusInfo) {
    this.startedAt = startedAt;
    this.configuration = configuration;
    this.iterationsStatusInfo = iterationsStatusInfo;
  }

  public OffsetDateTime getStartedAt() {
    return startedAt;
  }

  public ContainerBalancerConfiguration getConfiguration() {
    return configuration;
  }

  public List<ContainerBalancerTaskIterationStatusInfo> getIterationsStatusInfo() {
    return iterationsStatusInfo;
  }
}
