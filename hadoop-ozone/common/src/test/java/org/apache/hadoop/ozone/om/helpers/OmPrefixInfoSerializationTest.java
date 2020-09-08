package org.apache.hadoop.ozone.om.helpers;

import com.google.protobuf.ByteString;
import junit.framework.TestCase;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrefixInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class OmPrefixInfoSerializationTest extends TestCase {

  /**
   * Purpose of the test is to demonstrate how to bulid an OmPrefixInfo
   */

  public OzoneManagerProtocolProtos.OzoneAclInfo buildTestOzoneAclInfo(String aclString){
      OzoneAcl oacl = OzoneAcl.parseAcl(aclString);
      ByteString rights = ByteString.copyFrom(oacl.getAclBitSet().toByteArray());
      return OzoneManagerProtocolProtos.OzoneAclInfo.newBuilder()
          .setType(OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclType.USER)
          .setName(oacl.getName())
          .setRights(rights)
          .setAclScope(OzoneManagerProtocolProtos.OzoneAclInfo.OzoneAclScope.ACCESS)
          .build();
  }

  public HddsProtos.KeyValue getDefaultTestMetadata(String key, String value){
    return HddsProtos.KeyValue.newBuilder()
        .setKey(key)
        .setValue(value)
        .build();
  }

  public PrefixInfo getDefaultTestPrefixInfo(String name, String aclString, HddsProtos.KeyValue metadata){
    return PrefixInfo.newBuilder()
        .setName(name)
        .addAcls(buildTestOzoneAclInfo(aclString))
        .addMetadata(metadata)
        .build();
  }

  public boolean compareAcls(OzoneManagerProtocolProtos.OzoneAclInfo ozoneAclInfo, OzoneAcl acl) {
    boolean isEqual = true;
    isEqual = isEqual && ozoneAclInfo.getName() == acl.getName();
    return isEqual;
  }

  @Test
  public void testgetFromProtobufOneMetadataOneAcl(){
    String prefixInfoName = "MyPrefixInfoName";
    String aclString = "user:myuser:rw";
    String metakey = "metakey";
    String metaval = "metaval";
    HddsProtos.KeyValue metadata = getDefaultTestMetadata(metakey,metaval);
    PrefixInfo prefixInfo = getDefaultTestPrefixInfo(prefixInfoName, aclString,metadata);
    OmPrefixInfo ompri = OmPrefixInfo.getFromProtobuf(prefixInfo);
    Assert.assertEquals(prefixInfoName, ompri.getName() );
    Assert.assertEquals(1, ompri.getMetadata().size());
    Assert.assertEquals(metaval, ompri.getMetadata().get(metakey));
    Assert.assertEquals(1, ompri.getAcls().size());
    Assert.assertTrue(compareAcls(buildTestOzoneAclInfo(aclString),ompri.getAcls().get(0)));

  }
}