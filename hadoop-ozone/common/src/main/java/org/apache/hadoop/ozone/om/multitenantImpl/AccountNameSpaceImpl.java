package org.apache.hadoop.ozone.om.multitenantImpl;

import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.ozone.om.multitenant.AccountNameSpace;

public class AccountNameSpaceImpl implements AccountNameSpace {
  private final String accountNameSpaceID;

  public AccountNameSpaceImpl(String id) {
    accountNameSpaceID = id;
  }

  @Override
  public String getAccountNameSpaceID() {
    return accountNameSpaceID;
  }

  @Override
  public SpaceUsageSource getSpaceUsage() {
    return null;
  }

  @Override
  public void setQuota(OzoneQuota quota) {

  }

  @Override
  public OzoneQuota getQuota() {
    return null;
  }
}
