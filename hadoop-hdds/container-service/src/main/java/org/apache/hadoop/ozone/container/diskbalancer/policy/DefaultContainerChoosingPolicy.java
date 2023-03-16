package org.apache.hadoop.ozone.container.diskbalancer.policy;

import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

/**
 * Choose a container from specified volume, make sure it's not being balancing.
 */
public class DefaultContainerChoosingPolicy implements ContainerChoosingPolicy {
  public static final Logger LOG = LoggerFactory.getLogger(
      DefaultContainerChoosingPolicy.class);

  @Override
  public ContainerData chooseContainer(OzoneContainer ozoneContainer,
      HddsVolume hddsVolume, Set<Long> inProgressContainerIDs) {
    Iterator<Container<?>> itr = ozoneContainer.getController()
        .getContainers(hddsVolume);
    while (itr.hasNext()) {
      ContainerData containerData = itr.next().getContainerData();
      if (!inProgressContainerIDs.contains(
          containerData.getContainerID()) && containerData.isClosed()) {
        return containerData;
      }
    }
    return null;
  }
}
