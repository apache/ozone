package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetAddress;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

/**
 * Ozone Acl Wrapper class.
 */
public final class OzoneAclUtils {

  private OzoneAclUtils() {
  }

  /**
   * Check Acls of ozone object with volOwner given.
   * @param ozoneManager
   * @param resType
   * @param storeType
   * @param aclType
   * @param vol
   * @param bucket
   * @param key
   * @param volOwner
   * @param bucketOwner
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  public static void checkAllAcls(OzoneManager ozoneManager,
      OzoneObj.ResourceType resType,
      OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
      String vol, String bucket, String key, String volOwner,
      String bucketOwner, UserGroupInformation user, InetAddress remoteAddress,
      String hostName) throws IOException {

    boolean isVolOwner = isOwner(user, volOwner);

    IAccessAuthorizer.ACLType parentAclRight = aclType;

    if(ozoneManager.isNativeAuthorizerEnabled()) {
      if (aclType == IAccessAuthorizer.ACLType.CREATE ||
          aclType == IAccessAuthorizer.ACLType.DELETE ||
          aclType == IAccessAuthorizer.ACLType.WRITE_ACL) {
        parentAclRight = IAccessAuthorizer.ACLType.WRITE;
      } else if (aclType == IAccessAuthorizer.ACLType.READ_ACL ||
          aclType == IAccessAuthorizer.ACLType.LIST) {
        parentAclRight = IAccessAuthorizer.ACLType.READ;
      }
    } else {
      parentAclRight =  IAccessAuthorizer.ACLType.READ;

    }

    switch (resType) {
    case VOLUME:
      ozoneManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
          user, remoteAddress, hostName, true,
          volOwner);
      break;
    case BUCKET:
    case KEY:
    case PREFIX:
      ozoneManager.checkAcls(OzoneObj.ResourceType.VOLUME, storeType,
          parentAclRight, vol, bucket, key, user,
          remoteAddress, hostName, true,
          volOwner);
      if(isVolOwner){
        ozoneManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
            user, remoteAddress, hostName, true,
            volOwner);
      } else {
        ozoneManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
            user, remoteAddress, hostName, true,
            bucketOwner);
      }
      break;
    default:
      throw new OMException("Unexpected object type:" +
              resType, INVALID_REQUEST);
    }
  }

  private static boolean isOwner(UserGroupInformation callerUgi,
      String ownerName) {
    if (ownerName == null) {
      return false;
    }
    if (callerUgi.getUserName().equals(ownerName) ||
            callerUgi.getShortUserName().equals(ownerName)) {
      return true;
    }
    return false;
  }
}
