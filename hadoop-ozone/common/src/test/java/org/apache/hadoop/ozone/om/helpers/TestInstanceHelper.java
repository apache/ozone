package org.apache.hadoop.ozone.om.helpers;

import com.google.protobuf.ByteString;
import javafx.application.Application;
import javafx.stage.Stage;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

public class TestInstanceHelper {
  public static OzoneManagerProtocolProtos.OzoneAclInfo buildTestOzoneAclInfo(String aclString){
    OzoneAcl oacl = OzoneAcl.parseAcl(aclString);
    ByteString rights = ByteString.copyFrom(oacl.getAclBitSet().toByteArray());
    return OzoneManagerProtocolProtos.OzoneAclInfo.newBuilder()
        .setType(OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType.USER)
        .setName(oacl.getName())
        .setRights(rights)
        .setAclScope(OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope.ACCESS)
        .build();
  }

  public static HddsProtos.KeyValue getDefaultTestMetadata(String key, String value){
    return HddsProtos.KeyValue.newBuilder()
        .setKey(key)
        .setValue(value)
        .build();
  }

  public static OzoneManagerProtocolProtos.PrefixInfo getDefaultTestPrefixInfo(String name, String aclString, HddsProtos.KeyValue metadata){
    return OzoneManagerProtocolProtos.PrefixInfo.newBuilder()
        .setName(name)
        .addAcls(buildTestOzoneAclInfo(aclString))
        .addMetadata(metadata)
        .build();
  }
}
