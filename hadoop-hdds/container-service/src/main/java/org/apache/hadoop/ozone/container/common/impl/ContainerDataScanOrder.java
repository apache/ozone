/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.impl;

import java.time.Instant;
import java.util.Comparator;
import java.util.Optional;
import org.apache.hadoop.ozone.container.common.interfaces.Container;

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
