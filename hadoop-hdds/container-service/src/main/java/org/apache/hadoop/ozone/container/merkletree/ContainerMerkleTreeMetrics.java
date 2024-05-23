package org.apache.hadoop.ozone.container.merkletree;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableRate;

import java.util.function.Consumer;

public class ContainerMerkleTreeMetrics {
    private static final String METRICS_SOURCE_NAME = ContainerMerkleTreeMetrics.class.getSimpleName();
    public void updateMerkleTreeMetrics() {
        MetricsSystem ms = DefaultMetricsSystem.instance();
        ms.register(METRICS_SOURCE_NAME, "Container Merkle Tree Metrics", this);

    }

    public void unregister() {
        MetricsSystem ms = DefaultMetricsSystem.instance();
        ms.unregisterSource(METRICS_SOURCE_NAME);
    }

    public void incrementMerkleTreeWriteFailures() {

    }

    public MutableRate getWriteContainerMerkleTreeLatencyNS() {
        return null;
    }

    public void incrementMerkleTreeReadFailures() {
    }

    public MutableRate  getReadContainerMerkleTreeLatencyNS() {
        return null;
    }

    public void incrementMerkleTreeParseFailures() {

    }
}
