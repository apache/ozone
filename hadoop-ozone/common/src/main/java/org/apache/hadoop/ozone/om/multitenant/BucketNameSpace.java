package org.apache.hadoop.ozone.om.multitenant;

import java.util.List;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.ozone.security.acl.OzoneObj;

@InterfaceAudience.LimitedPrivate({"HDFS", "Yarn", "Ranger", "Hive", "HBase"})
@InterfaceStability.Evolving
public interface BucketNameSpace {
  /**
   * A Tenant will typically have his own BucketNameSpace to isolate the
   * the buckets of this Tenancy from that of others.
   * A BucketNameSpace can have different attributes and
   * restrictions that could apply to this BucketNameSpace.
   * Example of BucketNameSpace attributes can include
   * - Collective Space usage information across all the buckets in this
   * BucketNameSpace.
   * - Collective Quota restrictions across all the buckets of this
   * BucketNamespace.
   *
   * BucketNameSpace can be determined from the user context. Alternatively
   * APIs can use "BucketNameSpace:bucketName" naming convention.
   * Public buckets require unique bucket-names across all bucket-NameSpaces.
   *
   * Later, we can provide an API to create/set a public bucket by linking
   * the bucket in Tenant's bucketNameSpace to a globally unique bucket in
   * S3V(default-bucketNameSpace).
   *
   * @return BucketNameSpace-ID.
   */
  String getBucketNameSpaceID();

  /**
   * Returns all the top level Ozone objects that belong to a BucketNameSpace.
   * Some implementation can choose to represent it by Single Volume. Nothing
   * prevents any future extension where a bucketNameSpace can be multiple
   * volumes as well (Example Use case: one for each user).
   *
   * @return List of Ozone Volumes.
   */
  List<OzoneObj> getBucketNameSpaceObjects();

  /**
   * Add one or more volumes to this BucketNameSpace.
   * @param bucketNamespaceObject
   */
  void addBucketNameSpaceObjects(OzoneObj bucketNamespaceObject);

  /**
   * Get Space Usage Information for this BucketNameSpace. This can be
   * used for billing purpose. Such Aggregation can also be done lazily
   * by a Recon job. Implementations can decide.
   * @return
   */
  SpaceUsageSource getSpaceUsage();

  /**
   * Sets quota for this BucketNameSpace. Quota enforcement can also be done
   * Lazily by a Recon job but that would be a soft quota enforcement. Choice
   * of quota enforcement style is left to Implementation.
   * @param quota
   */
  void setQuota(OzoneQuota quota);

  /**
   * Get Quota Information for this BucketNameSpace.
   * @return
   */
  OzoneQuota getQuota();
}
