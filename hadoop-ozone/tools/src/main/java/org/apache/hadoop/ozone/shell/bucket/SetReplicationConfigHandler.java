package org.apache.hadoop.ozone.shell.bucket;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneIllegalArgumentException;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.SetReplicationConfigOptions;
import picocli.CommandLine;

import java.io.IOException;

/**
 * set replication configuration of the bucket.
 */
@CommandLine.Command(name = "set-replication-config",
    description = "Set replication config on bucket")
public class SetReplicationConfigHandler extends BucketHandler {

  @CommandLine.Mixin private SetReplicationConfigOptions
      replicationConfigOptions;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {
    if (Strings.isNullOrEmpty(replicationConfigOptions.getType()) || Strings
        .isNullOrEmpty(replicationConfigOptions.getReplication())) {
      throw new OzoneIllegalArgumentException(
          "Replication type or replication factor cannot be null.");
    }
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    OzoneBucket bucket =
        client.getObjectStore().getVolume(volumeName).getBucket(bucketName);
    bucket.setReplicationConfig(ReplicationConfig
        .parse(ReplicationType.valueOf(replicationConfigOptions.getType()),
            replicationConfigOptions.getReplication(),
            new OzoneConfiguration()));
  }
}
