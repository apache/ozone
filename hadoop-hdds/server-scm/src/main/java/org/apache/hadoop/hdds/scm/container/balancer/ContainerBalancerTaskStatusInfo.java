package org.apache.hadoop.hdds.scm.container.balancer;

import java.time.OffsetDateTime;
import java.util.List;

public class ContainerBalancerTaskStatusInfo {
    private final OffsetDateTime startedAt;
    private final ContainerBalancerConfiguration configuration;
    private final List<ContainerBalancerTaskIterationStatusInfo> iterationsStatusInfo;

    public ContainerBalancerTaskStatusInfo(OffsetDateTime startedAt, ContainerBalancerConfiguration configuration, List<ContainerBalancerTaskIterationStatusInfo> iterationsStatusInfo) {
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
