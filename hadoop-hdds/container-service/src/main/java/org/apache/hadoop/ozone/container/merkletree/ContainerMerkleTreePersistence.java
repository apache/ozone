package org.apache.hadoop.ozone.container.merkletree;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerMerkleTree;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.util.MetricUtil;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ContainerMerkleTreePersistence {
    public static String CONTAINER_MERKLE_TREE_FILE = "container.merkletree.proto";

    public static void writeMerkleTreeToDisk(ContainerData containerData,
                                             ContainerMerkleTree containerMerkleTree,
                                             ContainerMerkleTreeMetrics metrics) throws IOException {
        // Write the container Merkle Tree to disk.
        byte[] data = containerMerkleTree.toByteArray();
        // write the data to the file.
        try {
            MetricUtil.captureLatencyNs(metrics.getWriteContainerMerkleTreeLatencyNS(), () -> {
                Files.write(getFileNameForMerkleTree(containerData.getContainerPath()), data);
            });
        } catch (IOException ex) {
            metrics.incrementMerkleTreeWriteFailures();
            throw ex;
        }
    }

    private static Path getFileNameForMerkleTree(String path) {
        return Paths.get(path, CONTAINER_MERKLE_TREE_FILE);
    }

    public static ContainerMerkleTree readMerkleTreeFromDisk(ContainerData containerData,
                                                             ContainerMerkleTreeMetrics metrics) throws IOException {
        try {
            byte[] data = MetricUtil.captureLatencyNs(metrics.getReadContainerMerkleTreeLatencyNS(), () -> {
                return Files.readAllBytes(getFileNameForMerkleTree(containerData.getContainerPath()));
            });
            return ContainerMerkleTree.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            metrics.incrementMerkleTreeParseFailures();
            throw new RuntimeException(e);
        } catch (IOException ex) {
            metrics.incrementMerkleTreeReadFailures();
            throw ex;
        }
    }
}
