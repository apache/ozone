package org.apache.hadoop.ozone.kerberos;

import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestKerbName
{
  @BeforeEach
  public void setup() {
    KerberosName.setRuleMechanism("hadoop");
    KerberosName.setRules("RULE:[1:$1] RULE:[2:$1] DEFAULT");
  }

  @Test
  public void testSimplePrincipal() throws Exception {
    KerberosName name =
        new KerberosName("om@EXAMPLE.COM");
    String shortName = name.getShortName();
    assertEquals("om", shortName);
  }

  @Test
  public void testServicePrincipal() throws Exception {
    KerberosName name =
        new KerberosName("om/om@EXAMPLE.COM");
    String shortName = name.getShortName();
    assertEquals("om", shortName);
  }
}
