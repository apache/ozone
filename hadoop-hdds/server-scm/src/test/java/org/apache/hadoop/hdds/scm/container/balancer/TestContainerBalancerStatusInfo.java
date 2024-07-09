package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ContainerBalancerStatusInfo}.
 */
class TestContainerBalancerStatusInfo {

  @Test
  void testGetIterationStatistics() {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(20, OzoneConsts.GB));

    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);

    config.setIterations(2);
    config.setBalancingInterval(0);
    config.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);

    ContainerBalancerTask task = mockedScm.startBalancerTask(config);
    List<ContainerBalancerTaskIterationStatusInfo> iterationStatistics = task.getCurrentIterationsStatistic();
    assertEquals(3, iterationStatistics.size());
    iterationStatistics.forEach(is -> {
      assertTrue(is.getContainerMovesCompleted() > 0);
      assertEquals(0, is.getContainerMovesFailed());
      assertEquals(0, is.getContainerMovesTimeout());
      assertFalse(is.getSizeEnteringNodes().isEmpty());
      assertFalse(is.getSizeLeavingNodes().isEmpty());
    });

  }
}
