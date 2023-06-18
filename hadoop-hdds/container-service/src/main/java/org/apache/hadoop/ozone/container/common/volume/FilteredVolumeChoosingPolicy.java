/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;

/**
 * Volume choosing policy with support of pre-filtering of the DataNodes.
 */
public class FilteredVolumeChoosingPolicy implements VolumeChoosingPolicy {

  private final VolumeChoosingPolicy delegate;
  private final Predicate<HddsVolume> volumeFilter;

  /**
   *
   * @param delegate the volume choosing policy implementation that will do
   *                 the real work
   * @param volumeFilter predicate to pre-filter a list of {@link HddsVolume}
   */
  public FilteredVolumeChoosingPolicy(VolumeChoosingPolicy delegate,
                                      Predicate<HddsVolume> volumeFilter) {
    this.delegate = delegate;
    this.volumeFilter = volumeFilter;
  }

  @Override
  public HddsVolume chooseVolume(List<HddsVolume> volumes,
                                 long maxContainerSize) throws IOException {
    return delegate.chooseVolume(volumes.stream()
                                     .filter(volumeFilter)
                                     .collect(Collectors.toList()),
                                 maxContainerSize);
  }

  public VolumeChoosingPolicy getDelegate() {
    return delegate;
  }

}
