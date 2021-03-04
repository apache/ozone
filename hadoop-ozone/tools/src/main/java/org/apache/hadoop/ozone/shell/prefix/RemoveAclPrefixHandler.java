package org.apache.hadoop.ozone.shell.prefix;

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.acl.AclHandler;
import org.apache.hadoop.ozone.shell.acl.AclOption;
import picocli.CommandLine;

import java.io.IOException;

/**
 * Remove ACL from prefix.
 */
@CommandLine.Command(name = AclHandler.REMOVE_ACL_NAME,
    description = AclHandler.REMOVE_ACL_DESC)
public class RemoveAclPrefixHandler extends AclHandler {

  @CommandLine.Mixin
  private PrefixUri address;

  @CommandLine.Mixin
  private AclOption acls;

  @Override
  protected OzoneAddress getAddress() {
    return address.getValue();
  }

  @Override
  protected void execute(OzoneClient client, OzoneObj obj) throws IOException {
    acls.removeFrom(obj, client.getObjectStore(), out());
  }

}
