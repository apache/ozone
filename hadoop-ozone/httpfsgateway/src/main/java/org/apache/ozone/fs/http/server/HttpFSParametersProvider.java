/**
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
package org.apache.ozone.fs.http.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.ozone.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.ozone.lib.service.FileSystemAccess;
import org.apache.ozone.lib.wsrs.BooleanParam;
import org.apache.ozone.lib.wsrs.EnumParam;
import org.apache.ozone.lib.wsrs.EnumSetParam;
import org.apache.ozone.lib.wsrs.LongParam;
import org.apache.ozone.lib.wsrs.Param;
import org.apache.ozone.lib.wsrs.ParametersProvider;
import org.apache.ozone.lib.wsrs.ShortParam;
import org.apache.ozone.lib.wsrs.StringParam;
import org.apache.hadoop.util.StringUtils;

import javax.ws.rs.ext.Provider;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * HttpFS ParametersProvider.
 */
@Provider
@InterfaceAudience.Private
@SuppressWarnings("unchecked")
public class HttpFSParametersProvider extends ParametersProvider {

  private static final Map<Enum, Class<Param<?>>[]> PARAMS_DEF
      = new HashMap<Enum, Class<Param<?>>[]>();

  static {
    PARAMS_DEF.put(HttpFSFileSystem.Operation.OPEN,
        new Class[]{OffsetParam.class, LenParam.class, NoRedirectParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETFILESTATUS, new Class[]{});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.LISTSTATUS,
        new Class[]{FilterParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETHOMEDIRECTORY, new Class[]{});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETCONTENTSUMMARY, new Class[]{});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETQUOTAUSAGE, new Class[]{});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETFILECHECKSUM,
        new Class[]{NoRedirectParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETFILEBLOCKLOCATIONS,
        new Class[]{});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETACLSTATUS, new Class[]{});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETTRASHROOT, new Class[]{});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.INSTRUMENTATION, new Class[]{});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.APPEND,
        new Class[]{DataParam.class, NoRedirectParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.CONCAT,
        new Class[]{SourcesParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.TRUNCATE,
        new Class[]{NewLengthParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.CREATE,
        new Class[]{PermissionParam.class, OverwriteParam.class,
            ReplicationParam.class, BlockSizeParam.class, DataParam.class,
            UnmaskedPermissionParam.class, NoRedirectParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.MKDIRS,
        new Class[]{PermissionParam.class, UnmaskedPermissionParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.RENAME,
        new Class[]{DestinationParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.SETOWNER,
        new Class[]{OwnerParam.class, GroupParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.SETPERMISSION,
        new Class[]{PermissionParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.SETREPLICATION,
        new Class[]{ReplicationParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.SETTIMES,
        new Class[]{ModifiedTimeParam.class, AccessTimeParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.DELETE,
        new Class[]{RecursiveParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.SETACL,
        new Class[]{AclPermissionParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.REMOVEACL, new Class[]{});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.MODIFYACLENTRIES,
        new Class[]{AclPermissionParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.REMOVEACLENTRIES,
        new Class[]{AclPermissionParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.REMOVEDEFAULTACL, new Class[]{});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.SETXATTR,
        new Class[]{XAttrNameParam.class,
            XAttrValueParam.class,
            XAttrSetFlagParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.REMOVEXATTR,
        new Class[]{XAttrNameParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETXATTRS,
        new Class[]{XAttrNameParam.class, XAttrEncodingParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.LISTXATTRS, new Class[]{});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.LISTSTATUS_BATCH,
        new Class[]{StartAfterParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETALLSTORAGEPOLICY,
        new Class[] {});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETSTORAGEPOLICY,
        new Class[] {});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.SETSTORAGEPOLICY,
        new Class[] {PolicyNameParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.UNSETSTORAGEPOLICY,
        new Class[] {});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.ALLOWSNAPSHOT, new Class[] {});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.DISALLOWSNAPSHOT, new Class[] {});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.CREATESNAPSHOT,
            new Class[] {SnapshotNameParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.DELETESNAPSHOT,
            new Class[] {SnapshotNameParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.RENAMESNAPSHOT,
            new Class[] {OldSnapshotNameParam.class,
                SnapshotNameParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETSNAPSHOTDIFF,
        new Class[] {OldSnapshotNameParam.class,
            SnapshotNameParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETSNAPSHOTTABLEDIRECTORYLIST,
        new Class[] {});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETSERVERDEFAULTS,
        new Class[] {});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.CHECKACCESS,
        new Class[] {FsActionParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.SETECPOLICY,
        new Class[] {ECPolicyParam.class});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.GETECPOLICY, new Class[] {});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.UNSETECPOLICY, new Class[] {});
    PARAMS_DEF.put(HttpFSFileSystem.Operation.SATISFYSTORAGEPOLICY,
        new Class[] {});
  }

  public HttpFSParametersProvider() {
    super(HttpFSFileSystem.OP_PARAM, HttpFSFileSystem.Operation.class,
          PARAMS_DEF);
  }

  /**
   * Class for access-time parameter.
   */
  @InterfaceAudience.Private
  public static class AccessTimeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.ACCESS_TIME_PARAM;
    /**
     * Constructor.
     */
    public AccessTimeParam() {
      super(NAME, -1L);
    }
  }

  /**
   * Class for block-size parameter.
   */
  @InterfaceAudience.Private
  public static class BlockSizeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.BLOCKSIZE_PARAM;

    /**
     * Constructor.
     */
    public BlockSizeParam() {
      super(NAME, -1L);
    }
  }

  /**
   * Class for data parameter.
   */
  @InterfaceAudience.Private
  public static class DataParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "data";

    /**
     * Constructor.
     */
    public DataParam() {
      super(NAME, false);
    }
  }

  /**
   * Class for noredirect parameter.
   */
  @InterfaceAudience.Private
  public static class NoRedirectParam extends BooleanParam {
    /**
     * Parameter name.
     */
    public static final String NAME = "noredirect";
    /**
     * Constructor.
     */
    public NoRedirectParam() {
      super(NAME, false);
    }
  }

  /**
   * Class for operation parameter.
   */
  @InterfaceAudience.Private
  public static class OperationParam
      extends EnumParam<HttpFSFileSystem.Operation> {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OP_PARAM;
    /**
     * Constructor.
     */
    public OperationParam(String operation) {
      super(NAME, HttpFSFileSystem.Operation.class,
            HttpFSFileSystem.Operation.valueOf(
                StringUtils.toUpperCase(operation)));
    }
  }

  /**
   * Class for delete's recursive parameter.
   */
  @InterfaceAudience.Private
  public static class RecursiveParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.RECURSIVE_PARAM;

    /**
     * Constructor.
     */
    public RecursiveParam() {
      super(NAME, false);
    }
  }

  /**
   * Class for filter parameter.
   */
  @InterfaceAudience.Private
  public static class FilterParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "filter";

    /**
     * Constructor.
     */
    public FilterParam() {
      super(NAME, null);
    }

  }

  /**
   * Class for group parameter.
   */
  @InterfaceAudience.Private
  public static class GroupParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.GROUP_PARAM;

    /**
     * Constructor.
     */
    public GroupParam() {
      super(NAME, null);
    }

  }

  /**
   * Class for len parameter.
   */
  @InterfaceAudience.Private
  public static class LenParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "length";

    /**
     * Constructor.
     */
    public LenParam() {
      super(NAME, -1L);
    }
  }

  /**
   * Class for modified-time parameter.
   */
  @InterfaceAudience.Private
  public static class ModifiedTimeParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.MODIFICATION_TIME_PARAM;

    /**
     * Constructor.
     */
    public ModifiedTimeParam() {
      super(NAME, -1L);
    }
  }

  /**
   * Class for offset parameter.
   */
  @InterfaceAudience.Private
  public static class OffsetParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = "offset";

    /**
     * Constructor.
     */
    public OffsetParam() {
      super(NAME, 0L);
    }
  }

  /**
   * Class for newlength parameter.
   */
  @InterfaceAudience.Private
  public static class NewLengthParam extends LongParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.NEW_LENGTH_PARAM;

    /**
     * Constructor.
     */
    public NewLengthParam() {
      super(NAME, 0L);
    }
  }

  /**
   * Class for overwrite parameter.
   */
  @InterfaceAudience.Private
  public static class OverwriteParam extends BooleanParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OVERWRITE_PARAM;

    /**
     * Constructor.
     */
    public OverwriteParam() {
      super(NAME, true);
    }
  }

  /**
   * Class for owner parameter.
   */
  @InterfaceAudience.Private
  public static class OwnerParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OWNER_PARAM;

    /**
     * Constructor.
     */
    public OwnerParam() {
      super(NAME, null);
    }

  }

  /**
   * Class for permission parameter.
   */
  @InterfaceAudience.Private
  public static class PermissionParam extends ShortParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.PERMISSION_PARAM;


    /**
     * Constructor.
     */
    public PermissionParam() {
      super(NAME, HttpFSFileSystem.DEFAULT_PERMISSION, 8);
    }

  }

  /**
   * Class for unmaskedpermission parameter.
   */
  @InterfaceAudience.Private
  public static class UnmaskedPermissionParam extends ShortParam {

    /**
     * Parameter name.
     */
    public static final String NAME =
        HttpFSFileSystem.UNMASKED_PERMISSION_PARAM;


    /**
     * Constructor.
     */
    public UnmaskedPermissionParam() {
      super(NAME, (short) -1, 8);
    }

  }

  /**
   * Class for AclPermission parameter.
   */
  @InterfaceAudience.Private
  public static class AclPermissionParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.ACLSPEC_PARAM;

    /**
     * Constructor.
     */
    public AclPermissionParam() {
      super(NAME,
          HttpFSFileSystem.ACLSPEC_DEFAULT,
          Pattern.compile(HttpFSServerWebApp.get()
              .get(FileSystemAccess.class)
              .getFileSystemConfiguration()
              .get(HdfsClientConfigKeys.DFS_WEBHDFS_ACL_PERMISSION_PATTERN_KEY,
                  HdfsClientConfigKeys
                      .DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT)));
    }
  }

  /**
   * Class for replication parameter.
   */
  @InterfaceAudience.Private
  public static class ReplicationParam extends ShortParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.REPLICATION_PARAM;

    /**
     * Constructor.
     */
    public ReplicationParam() {
      super(NAME, (short) -1);
    }
  }

  /**
   * Class for concat sources parameter.
   */
  @InterfaceAudience.Private
  public static class SourcesParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.SOURCES_PARAM;

    /**
     * Constructor.
     */
    public SourcesParam() {
      super(NAME, null);
    }
  }

  /**
   * Class for to-path parameter.
   */
  @InterfaceAudience.Private
  public static class DestinationParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.DESTINATION_PARAM;

    /**
     * Constructor.
     */
    public DestinationParam() {
      super(NAME, null);
    }
  }
  
  /**
   * Class for xattr parameter.
   */
  @InterfaceAudience.Private
  public static class XAttrNameParam extends StringParam {
    public static final String XATTR_NAME_REGX = 
        "^(user\\.|trusted\\.|system\\.|security\\.).+";
    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.XATTR_NAME_PARAM;
    private static final Pattern PATTERN = Pattern.compile(XATTR_NAME_REGX);

    /**
     * Constructor.
     */
    public XAttrNameParam() {
      super(NAME, null, PATTERN);
    }
  }

  /**
   * Class for xattr parameter.
   */
  @InterfaceAudience.Private
  public static class XAttrValueParam extends StringParam {
    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.XATTR_VALUE_PARAM;

    /**
     * Constructor.
     */
    public XAttrValueParam() {
      super(NAME, null);
    }
  }

  /**
   * Class for xattr parameter.
   */
  @InterfaceAudience.Private
  public static class XAttrSetFlagParam extends EnumSetParam<XAttrSetFlag> {
    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.XATTR_SET_FLAG_PARAM;

    /**
     * Constructor.
     */
    public XAttrSetFlagParam() {
      super(NAME, XAttrSetFlag.class, null);
    }
  }

  /**
   * Class for xattr parameter.
   */
  @InterfaceAudience.Private
  public static class XAttrEncodingParam extends EnumParam<XAttrCodec> {
    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.XATTR_ENCODING_PARAM;

    /**
     * Constructor.
     */
    public XAttrEncodingParam() {
      super(NAME, XAttrCodec.class, null);
    }
  }

  /**
   * Class for startafter parameter.
   */
  @InterfaceAudience.Private
  public static class StartAfterParam extends StringParam {
    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.START_AFTER_PARAM;

    /**
     * Constructor.
     */
    public StartAfterParam() {
      super(NAME, null);
    }
  }

  /**
   * Class for policyName parameter.
   */
  @InterfaceAudience.Private
  public static class PolicyNameParam extends StringParam {
    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.POLICY_NAME_PARAM;

    /**
     * Constructor.
     */
    public PolicyNameParam() {
      super(NAME, null);
    }
  }

  /**
   * Class for SnapshotName parameter.
   */
  public static class SnapshotNameParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.SNAPSHOT_NAME_PARAM;

    /**
     * Constructor.
     */
    public SnapshotNameParam() {
      super(NAME, null);
    }

  }

  /**
   * Class for OldSnapshotName parameter.
   */
  public static class OldSnapshotNameParam extends StringParam {

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.OLD_SNAPSHOT_NAME_PARAM;

    /**
     * Constructor.
     */
    public OldSnapshotNameParam() {
      super(NAME, null);
    }
  }

  /**
   * Class for FsAction parameter.
   */
  @InterfaceAudience.Private
  public static class FsActionParam extends StringParam {

    private static final String FILE_SYSTEM_ACTION = "[r-][w-][x-]";
    private static final Pattern FSACTION_PATTERN =
        Pattern.compile(FILE_SYSTEM_ACTION);

    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.FSACTION_MODE_PARAM;

    /**
     * Constructor.
     */
    public FsActionParam() {
      super(NAME, null);
    }

    /**
     * Constructor.
     * @param str a string representation of the parameter value.
     */
    public FsActionParam(final String str) {
      super(NAME, str, FSACTION_PATTERN);
    }
  }

  /**
   * Class for ecpolicy parameter.
   */
  @InterfaceAudience.Private
  public static class ECPolicyParam extends StringParam {
    /**
     * Parameter name.
     */
    public static final String NAME = HttpFSFileSystem.EC_POLICY_NAME_PARAM;

    /**
     * Constructor.
     */
    public ECPolicyParam() {
      super(NAME, null);
    }
  }
}
