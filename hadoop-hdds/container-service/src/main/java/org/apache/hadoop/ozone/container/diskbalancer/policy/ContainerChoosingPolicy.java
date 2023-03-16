package org.apache.hadoop.ozone.container.diskbalancer.policy;

import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;

import java.util.Set;

/**
 * This interface specifies the policy for choosing containers to balance.
 */
public interface ContainerChoosingPolicy {
  /**
   * Choose a container for balancing.
   *
   * @return a Container
   */
  ContainerData chooseContainer(OzoneContainer ozoneContainer,
      HddsVolume volume, Set<Long> inProgressContainerIDs);
}
