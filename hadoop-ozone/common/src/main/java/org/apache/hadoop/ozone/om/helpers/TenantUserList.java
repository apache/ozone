package org.apache.hadoop.ozone.om.helpers;

import java.util.List;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantUserAccessId;

public class TenantUserList {

  private final String tenantName;

  private final List<TenantUserAccessId> userAccessIds;


  public TenantUserList(String tenantName,
                        List<TenantUserAccessId> userAccessIds) {
    this.tenantName = tenantName;
    this.userAccessIds = userAccessIds;
  }

  public String getTenantName() {
    return tenantName;
  }

  public List<TenantUserAccessId> getUserAccessIds() {
    return userAccessIds;
  }

  public static TenantUserList fromProtobuf(
      OzoneManagerProtocolProtos.TenantUserList tenantUserListInfo) {
    return new TenantUserList(tenantUserListInfo.getTenantName(),
        tenantUserListInfo.getUserAccessIdInfoList());
  }

  public OzoneManagerProtocolProtos.TenantUserList getProtobuf() {
    final OzoneManagerProtocolProtos.TenantUserList.Builder builder = OzoneManagerProtocolProtos.TenantUserList.newBuilder();
    builder.setTenantName(this.tenantName);
    userAccessIds.forEach(builder::addUserAccessIdInfo);
    return builder.build();
  }
}
