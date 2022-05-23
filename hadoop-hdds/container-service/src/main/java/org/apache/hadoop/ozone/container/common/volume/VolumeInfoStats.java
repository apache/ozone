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
 * This class is used to track Volume Info stats for each HDDS Volume.
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

    /**
     * @param identifier Typically, path to volume root. e.g. /data/hdds
     */
    public VolumeInfoStats(String identifier, HddsVolume ref) {
        this.metricsSourceName += '-' + identifier+ StorageVolume.VolumeType.DATA_VOLUME.toString();
        this.VolumeRootStr = identifier;
        this.volume = ref;
        init();
    }

    public void init() {
        MetricsSystem ms = DefaultMetricsSystem.instance();
        ms.register(metricsSourceName, "Volume Info Statistics", this);
    }

    public void unregister() {
        MetricsSystem ms = DefaultMetricsSystem.instance();
        ms.unregisterSource(metricsSourceName);
    }

    /**
     * Return the Total Available capacity of the Volume.
     */
    @Metric("Metric to return the Storage Type")
    public String getStorageType() {
        return volume.getStorageType().toString();
    }
    /**
     * Return the Storage Directory for the Volume
     */
    @Metric("Returns the Directory name for the volume")
    public String getStorageDirectory() {
        return volume.getStorageDir().toString();
    }

    /**
     * Return the DataNode UID for the respective volume
     */
    public String getDatanodeUuid() {
        return volume.getDatanodeUuid();
    }

    /**
     * Return the Layout version of the storage data
     */
    public int getLayoutVersion() {
        return volume.getLayoutVersion();
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
     * B) capacity = used + avail
     * C) Total capacity = used + avail + reserved
     */

    /**
     * Return the Storage type for the Volume
     */
    @Metric("Returns the Used space")
    public long getUsed() {
        return (volume.getVolumeInfo().getScmUsed());
    }

    /**
     * Return the Total Available capacity of the Volume.
     */
    @Metric("Returns the Available space")
    public long getAvailable() {
        return (volume.getVolumeInfo().getAvailable());
    }

    /**
     * Return the Total Reserved of the Volume.
     */
    @Metric("Fetches the Reserved Space")
    public long getReserved() {
        return (volume.getVolumeInfo().getReservedInBytes());
    }

    /**
     * Return the Total capacity of the Volume.
     */
    @Metric("Returns the Capacity of the Volume")
    public long getCapacity() {
        return spaceUsed.value() + spaceAvailable.value();
    }

    /**
     * Return the Total capacity of the Volume.
     */
    @Metric("Returns the Total Capacity of the Volume")
    public long getTotalCapacity() {
        return (spaceUsed.value() + spaceAvailable.value() + spaceReserved.value());
    }

}