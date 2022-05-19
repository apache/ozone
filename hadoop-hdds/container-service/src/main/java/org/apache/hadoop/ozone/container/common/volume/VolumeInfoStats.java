/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This class is used to track Volume IO stats for each HDDS Volume.
 */
@Metrics(about = "Ozone Volume Information Metrics", context = OzoneConsts.OZONE)
public class VolumeInfoStats {

    private String metricsSourceName = VolumeInfoStats.class.getSimpleName();
    private String VolumeRootStr;
    private HddsVolume volume;

    private @Metric MutableGaugeLong spaceUsed;
    private @Metric MutableGaugeLong spaceAvailable;
    private @Metric MutableGaugeLong spaceReserved;
    private @Metric MutableGaugeLong capacity;
    private @Metric MutableGaugeLong totalCapacity;


    @Deprecated
    public VolumeInfoStats() {
        init();
    }

    /**
     * @param identifier Typically, path to volume root. e.g. /data/hdds
     */
    public VolumeInfoStats(String identifier, HddsVolume ref) {
        this.metricsSourceName += '-' + identifier;
        this.VolumeRootStr = identifier;
        this.volume = ref;
        init();
    }

    public void init() {
        MetricsSystem ms = DefaultMetricsSystem.instance();
        ms.register(metricsSourceName, "Volume Info Statistics", this);
        spaceUsed.set(volume.getVolumeInfo().getScmUsed());
        spaceAvailable.set(volume.getVolumeInfo().getAvailable());
        spaceReserved.set(volume.getVolumeInfo().getReservedInBytes());
        capacity.set(spaceUsed.value() + spaceAvailable.value());
        totalCapacity.set(
                spaceUsed.value() + spaceAvailable.value() + spaceReserved.value());
    }

    public void unregister() {
        MetricsSystem ms = DefaultMetricsSystem.instance();
        ms.unregisterSource(metricsSourceName);
    }

    public String getMetricsSourceName() {
        return metricsSourceName;
    }

    /**
     * Test conservative avail space.
     * |----used----|   (avail)   |++++++++reserved++++++++|
     * |<------- capacity ------->|
     * |<------------------- Total capacity -------------->|
     * A) avail = capacity - used
     * B) avail = fsAvail - Max(reserved - other, 0);
     */

    /**
     * Return the Storage type for the Volume
     */
    public long getUsed() {
        spaceUsed.set(volume.getVolumeInfo().getScmUsed());
        return spaceUsed.value();
    }

    /**
     * Return the Total Available capacity of the Volume.
     */
    public long getAvailable() {
        spaceAvailable.set(volume.getVolumeInfo().getAvailable());
        return spaceAvailable.value();
    }

    /**
     * Return the Total Reserved of the Volume.
     */
    public long getReserved() {
        spaceReserved.set(volume.getVolumeInfo().getReservedInBytes());
        return spaceReserved.value();
    }

    /**
     * Return the Total capacity of the Volume.
     */
    public long getCapacity() {
        capacity.set(spaceUsed.value() + spaceAvailable.value());
        return capacity.value();
    }

    /**
     * Return the Total capacity of the Volume.
     */
    public long getTotalCapacity() {
        totalCapacity.set(
                spaceUsed.value() + spaceAvailable.value() + spaceReserved.value());
        return totalCapacity.value();
    }

}