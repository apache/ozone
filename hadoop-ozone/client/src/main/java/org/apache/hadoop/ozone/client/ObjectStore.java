/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.hadoop.security.token.Token;

/**
 * ObjectStore class is responsible for the client operations that can be
 * performed on Ozone Object Store.
 */
public class ObjectStore {

  /**
   * The proxy used for connecting to the cluster and perform
   * client operations.
   */
  // TODO: remove rest api and client
  private final ClientProtocol proxy;

  /**
   * Cache size to be used for listVolume calls.
   */
  private int listCacheSize;

  private String s3VolumeName;

  /**
   * Creates an instance of ObjectStore.
   * @param conf Configuration object.
   * @param proxy ClientProtocol proxy.
   */
  public ObjectStore(ConfigurationSource conf, ClientProtocol proxy) {
    this.proxy = TracingUtil.createProxy(proxy, ClientProtocol.class, conf);
    this.listCacheSize = HddsClientUtils.getListCacheSize(conf);
    this.s3VolumeName = HddsClientUtils.getS3VolumeName(conf);
  }

  @VisibleForTesting
  protected ObjectStore() {
    // For the unit test
    OzoneConfiguration conf = new OzoneConfiguration();
    this.s3VolumeName = HddsClientUtils.getS3VolumeName(conf);
    proxy = null;
  }

  @VisibleForTesting
  public ClientProtocol getClientProxy() {
    return proxy;
  }

  /**
   * Creates the volume with default values.
   * @param volumeName Name of the volume to be created.
   * @throws IOException
   */
  public void createVolume(String volumeName) throws IOException {
    proxy.createVolume(volumeName);
  }

  /**
   * Creates the volume.
   * @param volumeName Name of the volume to be created.
   * @param volumeArgs Volume properties.
   * @throws IOException
   */
  public void createVolume(String volumeName, VolumeArgs volumeArgs)
      throws IOException {
    proxy.createVolume(volumeName, volumeArgs);
  }

  /**
   * Creates an S3 bucket inside Ozone manager and creates the mapping needed
   * to access via both S3 and Ozone.
   * @param bucketName - S3 bucket Name.
   * @throws IOException - On failure, throws an exception like Bucket exists.
   */
  public void createS3Bucket(String bucketName) throws
      IOException {
    OzoneVolume volume = getVolume(s3VolumeName);
    volume.createBucket(bucketName);
  }

  public OzoneBucket getS3Bucket(String bucketName) throws IOException {
    return getVolume(s3VolumeName).getBucket(bucketName);
  }

  /**
   * Deletes an s3 bucket and removes mapping of Ozone volume/bucket.
   * @param bucketName - S3 Bucket Name.
   * @throws  IOException in case the bucket cannot be deleted.
   */
  public void deleteS3Bucket(String bucketName) throws IOException {
    try {
      OzoneVolume volume = getVolume(s3VolumeName);
      volume.deleteBucket(bucketName);
    } catch (OMException ex) {
      if (ex.getResult() == OMException.ResultCodes.VOLUME_NOT_FOUND) {
        throw new OMException(OMException.ResultCodes.BUCKET_NOT_FOUND);
      } else {
        throw ex;
      }
    }
  }


  /**
   * Returns the volume information.
   * @param volumeName Name of the volume.
   * @return OzoneVolume
   * @throws IOException
   */
  public OzoneVolume getVolume(String volumeName) throws IOException {
    OzoneVolume volume = proxy.getVolumeDetails(volumeName);
    return volume;
  }

  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    return proxy.getS3Secret(kerberosID);
  }

  public void revokeS3Secret(String kerberosID) throws IOException {
    proxy.revokeS3Secret(kerberosID);
  }

  /**
   * Returns Iterator to iterate over all the volumes in object store.
   * The result can be restricted using volume prefix, will return all
   * volumes if volume prefix is null.
   *
   * @param volumePrefix Volume prefix to match
   * @return {@code Iterator<OzoneVolume>}
   */
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix)
      throws IOException {
    return listVolumes(volumePrefix, null);
  }

  /**
   * Returns Iterator to iterate over all the volumes after prevVolume in object
   * store. If prevVolume is null it iterates from the first volume.
   * The result can be restricted using volume prefix, will return all
   * volumes if volume prefix is null.
   *
   * @param volumePrefix Volume prefix to match
   * @param prevVolume Volumes will be listed after this volume name
   * @return {@code Iterator<OzoneVolume>}
   */
  public Iterator<? extends OzoneVolume> listVolumes(String volumePrefix,
      String prevVolume) throws IOException {
    return new VolumeIterator(null, volumePrefix, prevVolume);
  }

  /**
   * Returns Iterator to iterate over the list of volumes after prevVolume
   * accessible by a specific user. The result can be restricted using volume
   * prefix, will return all volumes if volume prefix is null. If user is not
   * null, returns the volume of current user.
   *
   * Definition of accessible:
   * When ACL is enabled, accessible means the user has LIST permission.
   * When ACL is disabled, accessible means the user is the owner of the volume.
   * See {@code OzoneManager#listVolumeByUser}.
   *
   * @param user User Name
   * @param volumePrefix Volume prefix to match
   * @param prevVolume Volumes will be listed after this volume name
   * @return {@code Iterator<OzoneVolume>}
   */
  public Iterator<? extends OzoneVolume> listVolumesByUser(String user,
      String volumePrefix, String prevVolume)
      throws IOException {
    if(Strings.isNullOrEmpty(user)) {
      user = UserGroupInformation.getCurrentUser().getUserName();
    }
    return new VolumeIterator(user, volumePrefix, prevVolume);
  }

  /**
   * Deletes the volume.
   * @param volumeName Name of the volume.
   * @throws IOException
   */
  public void deleteVolume(String volumeName) throws IOException {
    proxy.deleteVolume(volumeName);
  }

  public KeyProvider getKeyProvider() throws IOException {
    return proxy.getKeyProvider();
  }

  public URI getKeyProviderUri() throws IOException {
    return proxy.getKeyProviderUri();
  }

  /**
   * An Iterator to iterate over {@link OzoneVolume} list.
   */
  private class VolumeIterator implements Iterator<OzoneVolume> {

    private String user = null;
    private String volPrefix = null;

    private Iterator<OzoneVolume> currentIterator;
    private OzoneVolume currentValue;

    /**
     * Creates an Iterator to iterate over all volumes after
     * prevVolume of the user. If prevVolume is null it iterates from the
     * first volume. The returned volumes match volume prefix.
     * @param user user name
     * @param volPrefix volume prefix to match
     */
    VolumeIterator(String user, String volPrefix, String prevVolume) {
      this.user = user;
      this.volPrefix = volPrefix;
      this.currentValue = null;
      this.currentIterator = getNextListOfVolumes(prevVolume).iterator();
    }

    @Override
    public boolean hasNext() {
      // IMPORTANT: Without this logic, remote iteration will not work.
      // Removing this will break the listVolume call if we try to
      // list more than 1000 (ozone.client.list.cache ) volumes.
      if (!currentIterator.hasNext() && currentValue != null) {
        currentIterator = getNextListOfVolumes(currentValue.getName())
            .iterator();
      }
      return currentIterator.hasNext();
    }

    @Override
    public OzoneVolume next() {
      if(hasNext()) {
        currentValue = currentIterator.next();
        return currentValue;
      }
      throw new NoSuchElementException();
    }

    /**
     * Returns the next set of volume list using proxy.
     * @param prevVolume previous volume, this will be excluded from the result
     * @return {@code List<OzoneVolume>}
     */
    private List<OzoneVolume> getNextListOfVolumes(String prevVolume) {
      try {
        //if user is null, we do list of all volumes.
        if(user != null) {
          return proxy.listVolumes(user, volPrefix, prevVolume, listCacheSize);
        }
        return proxy.listVolumes(volPrefix, prevVolume, listCacheSize);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Get a valid Delegation Token.
   *
   * @param renewer the designated renewer for the token
   * @return Token<OzoneDelegationTokenSelector>
   * @throws IOException
   */
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return proxy.getDelegationToken(renewer);
  }

  /**
   * Renew an existing delegation token.
   *
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   */
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    return proxy.renewDelegationToken(token);
  }

  /**
   * Cancel an existing delegation token.
   *
   * @param token delegation token
   * @throws IOException
   */
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws IOException {
    proxy.cancelDelegationToken(token);
  }

  /**
   * @return canonical service name of ozone delegation token.
   */
  public String getCanonicalServiceName() {
    return proxy.getCanonicalServiceName();
  }

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl to be added.
   * @return true if acl is added successfully, else false.
   * @throws IOException if there is error.
   * */
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    return proxy.addAcl(obj, acl);
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   *
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   * @return true if acl is added successfully, else false.
   * @throws IOException if there is error.
   */
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    return proxy.removeAcl(obj, acl);
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj Ozone object.
   * @param acls List of acls.
   * @return true if acl is added successfully, else false.
   * @throws IOException if there is error.
   */
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    return proxy.setAcl(obj, acls);
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @return true if acl is added successfully, else false.
   * @throws IOException if there is error.
   */
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    return proxy.getAcl(obj);
  }

}
