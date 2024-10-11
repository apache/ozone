package org.apache.hadoop.ozone.conf;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS_GROUPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ADMINISTRATORS_GROUPS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;

public final class OzoneS3ConfigUtils {
  static final Logger LOG = LoggerFactory.getLogger(OzoneS3ConfigUtils.class);

  private OzoneS3ConfigUtils() { }

  /**
   * Get the list of S3 administrators from Ozone config.
   *
   * @param conf An instance of {@link OzoneConfiguration} being used
   * @return A {@link Collection} of the S3 administrator users
   *
   * If ozone.s3.administrators value is empty string or unset,
   * defaults to ozone.administrators value.
   */
  public static Collection<String> getS3AdminsFromConfig(OzoneConfiguration conf) throws IOException {
    Collection<String> ozAdmins = conf.getTrimmedStringCollection(OZONE_S3_ADMINISTRATORS);

    if (ozAdmins == null || ozAdmins.isEmpty()) {
      ozAdmins = conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS);
    }
    String omSPN = UserGroupInformation.getCurrentUser().getShortUserName();
    if (!ozAdmins.contains(omSPN)) {
      ozAdmins.add(omSPN);
    }

    return ozAdmins;
  }

  /**
   * Get the list of the groups that are a part S3 administrators from Ozone config.
   *
   * @param conf An instance of {@link OzoneConfiguration} being used
   * @return A {@link Collection} of the S3 administrator groups
   *
   * If ozone.s3.administrators.groups value is empty or unset,
   * defaults to the ozone.administrators.groups value
   */
  public static Collection<String> getS3AdminsGroupsFromConfig(OzoneConfiguration conf) {
    Collection<String> s3AdminsGroup = conf.getTrimmedStringCollection(OZONE_S3_ADMINISTRATORS_GROUPS);

    if (s3AdminsGroup.isEmpty() && conf.getTrimmedStringCollection(OZONE_S3_ADMINISTRATORS).isEmpty()) {
      s3AdminsGroup = conf.getTrimmedStringCollection(OZONE_ADMINISTRATORS_GROUPS);
    }

    return s3AdminsGroup;
  }

  /**
   * Get the users and groups that are a part of S3 administrators.
   * @param conf  Stores an instance of {@link OzoneConfiguration} being used
   * @return an instance of {@link OzoneAdmins} containing the S3 admin users and groups
   */
  public static OzoneAdmins getS3Admins(OzoneConfiguration conf) {
    Collection<String> s3Admins;
    try {
      s3Admins = getS3AdminsFromConfig(conf);
    } catch (IOException ie) {
      s3Admins = null;
    }
    Collection<String> s3AdminGroups = getS3AdminsGroupsFromConfig(conf);

    if (LOG.isDebugEnabled()) {
      if (null == s3Admins) {
        LOG.debug("S3 Admins are not set in configuration");
      }
      if (null == s3AdminGroups) {
        LOG.debug("S3 Admin Groups are not set in configuration");
      }
    }
    return new OzoneAdmins(s3Admins, s3AdminGroups);
  }

  /**
   * Check if the provided user is an S3 administrator.
   * @param user An instance of {@link UserGroupInformation} with information about the user to verify
   * @param s3Admins An instance of {@link OzoneAdmins} containing information
   *                 of the S3 administrator users and groups in the system
   * @return {@code true} if the provided user is an S3 administrator else {@code false}
   */
  public static boolean isS3Admin(@Nullable UserGroupInformation user, OzoneAdmins s3Admins) {
    return null != user && s3Admins.isAdmin(user);
  }

  /**
   * Check if the provided user is an S3 administrator.
   * @param user An instance of {@link UserGroupInformation} with information about the user to verify
   * @param conf An instance of {@link OzoneConfiguration} being used
   * @return {@code true} if the provided user is an S3 administrator else {@code false}
   */
  public static boolean isS3Admin(@Nullable UserGroupInformation user, OzoneConfiguration conf) {
    OzoneAdmins s3Admins = getS3Admins(conf);
    return isS3Admin(user, s3Admins);
  }
}
