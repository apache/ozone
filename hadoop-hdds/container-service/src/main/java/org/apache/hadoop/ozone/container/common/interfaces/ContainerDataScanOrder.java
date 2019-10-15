package org.apache.hadoop.ozone.container.common.interfaces;

import org.apache.hadoop.ozone.container.common.impl.ContainerData;

import java.time.Instant;
import java.util.Comparator;
import java.util.Optional;

/**
 * Orders containers:
 * 1. containers not yet scanned first,
 * 2. then least recently scanned first,
 * 3. ties are broken by containerID.
 */
public class ContainerDataScanOrder implements Comparator<Container<?>> {

  public static final Comparator<Container<?>> INSTANCE =
      new ContainerDataScanOrder();

  @Override
  public int compare(Container<?> o1, Container<?> o2) {
    ContainerData d1 = o1.getContainerData();
    ContainerData d2 = o2.getContainerData();

    Optional<Instant> scan1 = d1.lastDataScanTime();
    boolean scanned1 = scan1.isPresent();
    Optional<Instant> scan2 = d2.lastDataScanTime();
    boolean scanned2 = scan2.isPresent();

    int result = Boolean.compare(scanned1, scanned2);
    if (0 == result && scanned1 && scanned2) {
      result = scan1.get().compareTo(scan2.get());
    }
    if (0 == result) {
      result = Long.compare(d1.getContainerID(), d2.getContainerID());
    }

    return result;
  }
}
