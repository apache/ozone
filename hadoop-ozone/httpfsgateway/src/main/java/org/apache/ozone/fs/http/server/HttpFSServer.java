/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ozone.fs.http.server;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.apache.ozone.fs.http.HttpFSConstants;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.AclPermissionParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.BlockSizeParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.DataParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.DestinationParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.ECPolicyParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.FilterParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.FsActionParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.LenParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.NewLengthParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.NoRedirectParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.OffsetParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.OldSnapshotNameParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.OperationParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.OverwriteParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.PermissionParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.PolicyNameParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.RecursiveParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.ReplicationParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.SnapshotNameParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.SourcesParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.UnmaskedPermissionParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.XAttrEncodingParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.XAttrNameParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.XAttrSetFlagParam;
import org.apache.ozone.fs.http.server.HttpFSParametersProvider.XAttrValueParam;
import org.apache.ozone.lib.service.FileSystemAccess;
import org.apache.ozone.lib.service.FileSystemAccessException;
import org.apache.ozone.lib.service.Groups;
import org.apache.ozone.lib.service.Instrumentation;
import org.apache.ozone.lib.servlet.FileSystemReleaseFilter;
import org.apache.ozone.lib.wsrs.InputStreamEntity;
import org.apache.ozone.lib.wsrs.Parameters;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Main class of HttpFSServer server.
 * <p>
 * The <code>HttpFSServer</code> class uses Jersey JAX-RS to binds HTTP
 * requests to the different operations.
 */
@Path(HttpFSConstants.SERVICE_VERSION)
@InterfaceAudience.Private
public class HttpFSServer {

  private static final Logger AUDIT_LOG
      = LoggerFactory.getLogger("httpfsaudit");
  private static final Logger LOG = LoggerFactory.getLogger(HttpFSServer.class);

  private static final HttpFSParametersProvider PARAMETERS_PROVIDER =
      new HttpFSParametersProvider();

  private AccessMode accessMode = AccessMode.READWRITE;

  public HttpFSServer() {
    Configuration conf = HttpFSServerWebApp.get().getConfig();
    final String accessModeString
        = conf.get("httpfs.access.mode", "read-write")
        .toLowerCase();
    if (accessModeString.compareTo("write-only") == 0) {
      accessMode = AccessMode.WRITEONLY;
    } else if (accessModeString.compareTo("read-only") == 0) {
      accessMode = AccessMode.READONLY;
    } else {
      accessMode = AccessMode.READWRITE;
    }
  }

  private Parameters getParams(HttpServletRequest request) {
    return PARAMETERS_PROVIDER.get(request);
  }

  /**
   * Executes a {@link FileSystemAccess.FileSystemExecutor} using a filesystem
   * for the effective user.
   *
   * @param ugi user making the request.
   * @param executor FileSystemExecutor to execute.
   *
   * @return FileSystemExecutor response
   *
   * @throws IOException thrown if an IO error occurs.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  private <T> T fsExecute(UserGroupInformation ugi,
                          FileSystemAccess.FileSystemExecutor<T> executor)
      throws IOException, FileSystemAccessException {
    FileSystemAccess fsAccess
        = HttpFSServerWebApp.get().get(FileSystemAccess.class);
    Configuration conf = HttpFSServerWebApp.get().get(FileSystemAccess.class)
        .getFileSystemConfiguration();
    return fsAccess.execute(ugi.getShortUserName(), conf, executor);
  }

  /**
   * Returns a filesystem instance. The fileystem instance is wired for release
   * at the completion of the current Servlet request via the
   * {@link FileSystemReleaseFilter}.
   * <p>
   * If a do-as user is specified, the current user must be a valid proxyuser,
   * otherwise an <code>AccessControlException</code> will be thrown.
   *
   * @param ugi principal for whom the filesystem instance is.
   *
   * @return a filesystem for the specified user or do-as user.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  private FileSystem createFileSystem(UserGroupInformation ugi)
      throws IOException, FileSystemAccessException {
    String hadoopUser = ugi.getShortUserName();
    FileSystemAccess fsAccess = HttpFSServerWebApp.get()
        .get(FileSystemAccess.class);
    Configuration conf = HttpFSServerWebApp.get().get(FileSystemAccess.class)
        .getFileSystemConfiguration();
    FileSystem fs = fsAccess.createFileSystem(hadoopUser, conf);
    FileSystemReleaseFilter.setFileSystem(fs);
    return fs;
  }

  private void enforceRootPath(HttpFSConstants.Operation op, String path) {
    if (!path.equals("/")) {
      throw new UnsupportedOperationException(
        MessageFormat.format("Operation [{0}], invalid path [{1}], must be '/'",
                             op, path));
    }
  }

  /**
   * Special binding for '/' as it is not handled by the wildcard binding.
   *
   * @param uriInfo uri info of the request.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response getRoot(@Context UriInfo uriInfo,
                          @QueryParam(OperationParam.NAME) OperationParam op,
                          @Context HttpServletRequest request)
      throws IOException, FileSystemAccessException {
    return get("", uriInfo, op, request);
  }

  private String makeAbsolute(String path) {
    return "/" + ((path != null) ? path : "");
  }

  /**
   * Binding to handle GET requests, supported operations are.
   *
   * @param path the path for operation.
   * @param uriInfo uri info of the request.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  @GET
  @Path("{path:.*}")
  @Produces({MediaType.APPLICATION_OCTET_STREAM + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
  public Response get(@PathParam("path") String path,
                      @Context UriInfo uriInfo,
                      @QueryParam(OperationParam.NAME) OperationParam op,
                      @Context HttpServletRequest request)
      throws IOException, FileSystemAccessException,
      UnsupportedOperationException {
    // Restrict access to only GETFILESTATUS and LISTSTATUS in write-only mode
    if ((op.value() != HttpFSConstants.Operation.GETFILESTATUS) &&
            (op.value() != HttpFSConstants.Operation.LISTSTATUS) &&
            accessMode == AccessMode.WRITEONLY) {
      return Response.status(Response.Status.FORBIDDEN).build();
    }
    UserGroupInformation user = HttpUserGroupInformation.get();
    final Parameters params = getParams(request);
    Response response;
    path = makeAbsolute(path);
    MDC.put(HttpFSConstants.OP_PARAM, op.value().name());
    MDC.put("hostname", request.getRemoteAddr());
    switch (op.value()) {
    case OPEN:
      response = handleOpen(path, uriInfo, params, user);
      break;
    case GETFILESTATUS:
      response = handleGetFileStatus(path, user);
      break;
    case LISTSTATUS:
      response = handleListStatus(path, params, user);
      break;
    case GETHOMEDIRECTORY:
      throw new UnsupportedOperationException(getClass().getSimpleName()
          + " doesn't support GETHOMEDIRECTORY");
      //response = handleGetHomeDir(path, op, user);
      //break;
    case INSTRUMENTATION:
      response = handleInstrumentation(path, op, user);
      break;
    case GETCONTENTSUMMARY:
      response = handleGetContentSummary(path, user);
      break;
    case GETQUOTAUSAGE:
      response = handleGetQuotaUsage(path, user);
      break;
    case GETFILECHECKSUM:
      throw new UnsupportedOperationException(getClass().getSimpleName()
          + " doesn't support GETFILECHECKSUM");
      // response = handleGetFileCheckSum(path, uriInfo, params, user);
      // break;
    case GETFILEBLOCKLOCATIONS:
      response = Response.status(Response.Status.BAD_REQUEST).build();
      break;
    case GETACLSTATUS:
      response = handleGetACLStatus(path, user);
      break;
    case GETXATTRS:
      response = handleGetXAttrs(path, params, user);
      break;
    case LISTXATTRS:
      response = handleListXAttrs(path, user);
      break;
    case LISTSTATUS_BATCH:
      throw new UnsupportedOperationException(getClass().getSimpleName()
          + " doesn't support LISTSTATUS_BATCH");
      //response = handleListStatusBatch(path, params, user);
      //break;
    case GETTRASHROOT:
      throw new UnsupportedOperationException(getClass().getSimpleName()
          + " doesn't support GETTRASHROOT");
      //response = handleGetTrashRoot(path, user);
      //break;
    case GETALLSTORAGEPOLICY:
      response = handleGetAllStoragePolicy(path, user);
      break;
    case GETSTORAGEPOLICY:
      response = handleGetStoragePolicy(path, user);
      break;
    case GETSNAPSHOTDIFF:
      response = handleGetSnapshotDiff(path, params, user);
      break;
    case GETSNAPSHOTTABLEDIRECTORYLIST:
      response = handleGetSnaphotTableDirectoryList(user);
      break;
    case GETSERVERDEFAULTS:
      response = handleGetServerDefaults(user);
      break;
    case CHECKACCESS:
      response = handleCheckAccess(path, params, user);
      break;
    case GETECPOLICY:
      response = handleGetECPolicy(path, user);
      break;
    default:
      throw new IOException(
          MessageFormat.format("Invalid HTTP GET operation [{0}]", op.value()));
    }
    return response;
  }

  private Response handleGetECPolicy(String path, UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSGetErasureCodingPolicy command =
        new FSOperations.FSGetErasureCodingPolicy(path);
    String js = fsExecute(user, command);
    AUDIT_LOG.info("[{}]", path);
    response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleCheckAccess(String path,
                                     Parameters params,
                                     UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String mode = params.get(FsActionParam.NAME, FsActionParam.class);
    FsActionParam fsparam = new FsActionParam(mode);
    FSOperations.FSAccess command = new FSOperations.FSAccess(path,
        FsAction.getFsAction(fsparam.value()));
    fsExecute(user, command);
    AUDIT_LOG.info("[{}]", "/");
    response = Response.ok().build();
    return response;
  }

  private Response handleGetServerDefaults(UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSGetServerDefaults command =
        new FSOperations.FSGetServerDefaults();
    String js = fsExecute(user, command);
    AUDIT_LOG.info("[{}]", "/");
    response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleGetSnaphotTableDirectoryList(UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSGetSnapshottableDirListing command =
        new FSOperations.FSGetSnapshottableDirListing();
    String js = fsExecute(user, command);
    AUDIT_LOG.info("[{}]", "/");
    response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleGetSnapshotDiff(String path,
                                         Parameters params,
                                         UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String oldSnapshotName = params.get(OldSnapshotNameParam.NAME,
        OldSnapshotNameParam.class);
    String snapshotName = params.get(SnapshotNameParam.NAME,
        SnapshotNameParam.class);
    FSOperations.FSGetSnapshotDiff command =
        new FSOperations.FSGetSnapshotDiff(path, oldSnapshotName,
            snapshotName);
    String js = fsExecute(user, command);
    AUDIT_LOG.info("[{}]", path);
    response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleGetStoragePolicy(String path,
                                          UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSGetStoragePolicy command =
        new FSOperations.FSGetStoragePolicy(path);
    JSONObject json = fsExecute(user, command);
    AUDIT_LOG.info("[{}]", path);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleGetAllStoragePolicy(String path,
                                             UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSGetAllStoragePolicies command =
        new FSOperations.FSGetAllStoragePolicies();
    JSONObject json = fsExecute(user, command);
    AUDIT_LOG.info("[{}]", path);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleListXAttrs(String path, UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSListXAttrs command = new FSOperations.FSListXAttrs(path);
    @SuppressWarnings("rawtypes") Map json = fsExecute(user, command);
    AUDIT_LOG.info("XAttr names for [{}]", path);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleGetXAttrs(String path,
                                   Parameters params,
                                   UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    List<String> xattrNames =
        params.getValues(XAttrNameParam.NAME, XAttrNameParam.class);
    XAttrCodec encoding =
        params.get(XAttrEncodingParam.NAME, XAttrEncodingParam.class);
    FSOperations.FSGetXAttrs command =
        new FSOperations.FSGetXAttrs(path, xattrNames, encoding);
    @SuppressWarnings("rawtypes") Map json = fsExecute(user, command);
    AUDIT_LOG.info("XAttrs for [{}]", path);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleGetACLStatus(String path, UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSAclStatus command = new FSOperations.FSAclStatus(path);
    Map json = fsExecute(user, command);
    AUDIT_LOG.info("ACL status for [{}]", path);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleGetQuotaUsage(String path, UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSQuotaUsage command =
        new FSOperations.FSQuotaUsage(path);
    Map json = fsExecute(user, command);
    AUDIT_LOG.info("Quota Usage for [{}]", path);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleGetContentSummary(String path,
                                           UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSContentSummary command =
        new FSOperations.FSContentSummary(path);
    Map json = fsExecute(user, command);
    AUDIT_LOG.info("Content summary for [{}]", path);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleInstrumentation(String path,
                                         OperationParam op,
                                         UserGroupInformation user)
      throws IOException {
    Response response;
    enforceRootPath(op.value(), path);
    Groups groups = HttpFSServerWebApp.get().get(Groups.class);
    List<String> userGroups = groups.getGroups(user.getShortUserName());
    if (!userGroups.contains(HttpFSServerWebApp.get().getAdminGroup())) {
      throw new AccessControlException(
          "User not in HttpFSServer admin group");
    }
    Instrumentation instrumentation =
        HttpFSServerWebApp.get().get(Instrumentation.class);
    Map snapshot = instrumentation.getSnapshot();
    response = Response.ok(snapshot).build();
    return response;
  }

  private Response handleListStatus(String path,
                                    Parameters params,
                                    UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String filter = params.get(FilterParam.NAME, FilterParam.class);
    FSOperations.FSListStatus command =
        new FSOperations.FSListStatus(path, filter);
    Map json = fsExecute(user, command);
    AUDIT_LOG.info("[{}] filter [{}]", path, (filter != null) ? filter : "-");
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleGetFileStatus(String path, UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSFileStatus command = new FSOperations.FSFileStatus(path);
    Map json = fsExecute(user, command);
    AUDIT_LOG.info("[{}]", path);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleOpen(String path,
                              UriInfo uriInfo,
                              Parameters params,
                              UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    Boolean noRedirect = params
        .get(NoRedirectParam.NAME, NoRedirectParam.class);
    if (noRedirect) {
      URI redirectURL = createOpenRedirectionURL(uriInfo);
      final String js = JsonUtil.toJsonString("Location", redirectURL);
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    } else {
      //Invoking the command directly using an unmanaged FileSystem that is
      // released by the FileSystemReleaseFilter
      final FSOperations.FSOpen command = new FSOperations.FSOpen(path);
      final FileSystem fs = createFileSystem(user);
      InputStream is = null;
      UserGroupInformation ugi = UserGroupInformation
          .createProxyUser(user.getShortUserName(),
              UserGroupInformation.getLoginUser());
      try {
        is = ugi.doAs(new PrivilegedExceptionAction<InputStream>() {
          @Override
          public InputStream run() throws Exception {
            return command.execute(fs);
          }
        });
      } catch (InterruptedException ie) {
        LOG.warn("Open interrupted.", ie);
        Thread.currentThread().interrupt();
      }
      Long offset = params.get(OffsetParam.NAME, OffsetParam.class);
      Long len = params.get(LenParam.NAME, LenParam.class);
      AUDIT_LOG.info("[{}] offset [{}] len [{}]",
          new Object[]{path, offset, len});
      InputStreamEntity entity = new InputStreamEntity(is, offset, len);
      response = Response.ok(entity).type(MediaType.APPLICATION_OCTET_STREAM)
          .build();
    }
    return response;
  }

  /**
   * Create an open redirection URL from a request. It points to the same
   * HttpFS endpoint but removes the "redirect" parameter.
   * @param uriInfo uri info of the request.
   * @return URL for the redirected location.
   */
  private URI createOpenRedirectionURL(UriInfo uriInfo) {
    UriBuilder uriBuilder = uriInfo.getRequestUriBuilder();
    uriBuilder.replaceQueryParam(NoRedirectParam.NAME, (Object[])null);
    return uriBuilder.build((Object[])null);
  }

  /**
   * Binding to handle DELETE requests.
   *
   * @param path the path for operation.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  @DELETE
  @Path("{path:.*}")
  @Produces(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8)
  public Response delete(@PathParam("path") String path,
                         @QueryParam(OperationParam.NAME) OperationParam op,
                         @Context HttpServletRequest request)
      throws IOException, FileSystemAccessException {
    // Do not allow DELETE commands in read-only mode
    if (accessMode == AccessMode.READONLY) {
      return Response.status(Response.Status.FORBIDDEN).build();
    }
    UserGroupInformation user = HttpUserGroupInformation.get();
    final Parameters params = getParams(request);
    Response response;
    path = makeAbsolute(path);
    MDC.put(HttpFSConstants.OP_PARAM, op.value().name());
    MDC.put("hostname", request.getRemoteAddr());
    switch (op.value()) {
    case DELETE:
      response = handleDelete(path, params, user);
      break;
    case DELETESNAPSHOT:
      response = handleDeleteSnapshot(path, params, user);
      break;
    default:
      throw new IOException(
        MessageFormat.format("Invalid HTTP DELETE operation [{0}]",
                             op.value()));
    }
    return response;
  }

  private Response handleDeleteSnapshot(String path,
                                        Parameters params,
                                        UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String snapshotName = params.get(SnapshotNameParam.NAME,
        SnapshotNameParam.class);
    FSOperations.FSDeleteSnapshot command
        = new FSOperations.FSDeleteSnapshot(path, snapshotName);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] deleted snapshot [{}]", path, snapshotName);
    response = Response.ok().build();
    return response;
  }

  private Response handleDelete(String path,
                                Parameters params,
                                UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    Boolean recursive
        = params.get(RecursiveParam.NAME,  RecursiveParam.class);
    AUDIT_LOG.info("[{}] recursive [{}]", path, recursive);
    FSOperations.FSDelete command
        = new FSOperations.FSDelete(path, recursive);
    JSONObject json = fsExecute(user, command);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  /**
   * Special binding for '/' as it is not handled by the wildcard binding.
   * @param is the inputstream for the request payload.
   * @param uriInfo the of the request.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   *           handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess related
   *           error occurred. Thrown exceptions are handled by
   *           {@link HttpFSExceptionProvider}.
   */
  @POST
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8 })
  public Response postRoot(InputStream is, @Context UriInfo uriInfo,
      @QueryParam(OperationParam.NAME) OperationParam op,
      @Context HttpServletRequest request)
      throws IOException, FileSystemAccessException {
    return post(is, uriInfo, "/", op, request);
  }

  /**
   * Binding to handle POST requests.
   *
   * @param is the inputstream for the request payload.
   * @param uriInfo the of the request.
   * @param path the path for operation.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  @POST
  @Path("{path:.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
  public Response post(InputStream is,
                       @Context UriInfo uriInfo,
                       @PathParam("path") String path,
                       @QueryParam(OperationParam.NAME) OperationParam op,
                       @Context HttpServletRequest request)
      throws IOException, FileSystemAccessException {
    // Do not allow POST commands in read-only mode
    if (accessMode == AccessMode.READONLY) {
      return Response.status(Response.Status.FORBIDDEN).build();
    }
    UserGroupInformation user = HttpUserGroupInformation.get();
    final Parameters params = getParams(request);
    Response response;
    path = makeAbsolute(path);
    MDC.put(HttpFSConstants.OP_PARAM, op.value().name());
    MDC.put("hostname", request.getRemoteAddr());
    switch (op.value()) {
    case APPEND:
      response = handleAppend(is, uriInfo, path, params, user);
      break;
    case CONCAT:
      response = handleConcat(path, params, user);
      break;
    case TRUNCATE:
      response = handleTruncate(path, params, user);
      break;
    case UNSETSTORAGEPOLICY:
      response = handleUnsetStoragePolicy(path, user);
      break;
    case UNSETECPOLICY:
      response = handleUnsetECPolicy(path, user);
      break;
    default:
      throw new IOException(
        MessageFormat.format("Invalid HTTP POST operation [{0}]",
                             op.value()));
    }
    return response;
  }

  private Response handleUnsetECPolicy(String path,
                                       UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSUnSetErasureCodingPolicy command
        = new FSOperations.FSUnSetErasureCodingPolicy(path);
    fsExecute(user, command);
    AUDIT_LOG.info("Unset ec policy [{}]", path);
    response = Response.ok().build();
    return response;
  }

  private Response handleUnsetStoragePolicy(String path,
                                            UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSUnsetStoragePolicy command
        = new FSOperations.FSUnsetStoragePolicy(path);
    fsExecute(user, command);
    AUDIT_LOG.info("Unset storage policy [{}]", path);
    response = Response.ok().build();
    return response;
  }

  private Response handleTruncate(String path,
                                  Parameters params,
                                  UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    Long newLength = params.get(NewLengthParam.NAME, NewLengthParam.class);
    FSOperations.FSTruncate command
        = new FSOperations.FSTruncate(path, newLength);
    JSONObject json = fsExecute(user, command);
    AUDIT_LOG.info("Truncate [{}] to length [{}]", path, newLength);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleConcat(String path,
                                Parameters params,
                                UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String sources = params.get(SourcesParam.NAME, SourcesParam.class);
    FSOperations.FSConcat command
        = new FSOperations.FSConcat(path, sources.split(","));
    fsExecute(user, command);
    AUDIT_LOG.info("[{}]", path);
    response = Response.ok().build();
    return response;
  }

  private Response handleAppend(InputStream is,
                                UriInfo uriInfo,
                                String path,
                                Parameters params,
                                UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    Boolean hasData = params.get(DataParam.NAME, DataParam.class);
    URI redirectURL = createUploadRedirectionURL(uriInfo,
        HttpFSConstants.Operation.APPEND);
    Boolean noRedirect
        = params.get(NoRedirectParam.NAME, NoRedirectParam.class);
    if (noRedirect) {
      final String js = JsonUtil.toJsonString("Location", redirectURL);
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    } else if (hasData) {
      FSOperations.FSAppend command
          = new FSOperations.FSAppend(is, path);
      fsExecute(user, command);
      AUDIT_LOG.info("[{}]", path);
      response = Response.ok().type(MediaType.APPLICATION_JSON).build();
    } else {
      response = Response.temporaryRedirect(redirectURL).build();
    }
    return response;
  }

  /**
   * Creates the URL for an upload operation (create or append).
   *
   * @param uriInfo uri info of the request.
   * @param uploadOperation operation for the upload URL.
   *
   * @return the URI for uploading data.
   */
  protected URI createUploadRedirectionURL(UriInfo uriInfo,
                                           Enum<?> uploadOperation) {
    UriBuilder uriBuilder = uriInfo.getRequestUriBuilder();
    uriBuilder = uriBuilder.replaceQueryParam(OperationParam.NAME,
        uploadOperation)
        .queryParam(DataParam.NAME, Boolean.TRUE)
        .replaceQueryParam(NoRedirectParam.NAME, (Object[]) null);
    return uriBuilder.build(null);
  }

  /**
   * Special binding for '/' as it is not handled by the wildcard binding.
   * @param is the inputstream for the request payload.
   * @param uriInfo the of the request.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   *           handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess related
   *           error occurred. Thrown exceptions are handled by
   *           {@link HttpFSExceptionProvider}.
   */
  @PUT
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8 })
  public Response putRoot(InputStream is, @Context UriInfo uriInfo,
      @QueryParam(OperationParam.NAME) OperationParam op,
      @Context HttpServletRequest request)
      throws IOException, FileSystemAccessException {
    return put(is, uriInfo, "/", op, request);
  }

  /**
   * Binding to handle PUT requests.
   *
   * @param is the inputstream for the request payload.
   * @param uriInfo the of the request.
   * @param path the path for operation.
   * @param op the HttpFS operation of the request.
   *
   * @return the request response.
   *
   * @throws IOException thrown if an IO error occurred. Thrown exceptions are
   * handled by {@link HttpFSExceptionProvider}.
   * @throws FileSystemAccessException thrown if a FileSystemAccess releated
   * error occurred. Thrown exceptions are handled by
   * {@link HttpFSExceptionProvider}.
   */
  @PUT
  @Path("{path:.*}")
  @Consumes({"*/*"})
  @Produces({MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
  public Response put(InputStream is,
                       @Context UriInfo uriInfo,
                       @PathParam("path") String path,
                       @QueryParam(OperationParam.NAME) OperationParam op,
                       @Context HttpServletRequest request)
      throws IOException, FileSystemAccessException {
    // Do not allow PUT commands in read-only mode
    if (accessMode == AccessMode.READONLY) {
      return Response.status(Response.Status.FORBIDDEN).build();
    }
    UserGroupInformation user = HttpUserGroupInformation.get();
    final Parameters params = getParams(request);
    Response response;
    path = makeAbsolute(path);
    MDC.put(HttpFSConstants.OP_PARAM, op.value().name());
    MDC.put("hostname", request.getRemoteAddr());
    switch (op.value()) {
    case CREATE:
      response = handleCreate(is, uriInfo, path, params, user);
      break;
    case ALLOWSNAPSHOT:
      response = handleAllowSnapshot(path, user);
      break;
    case DISALLOWSNAPSHOT:
      response = handleDisallowSnapshot(path, user);
      break;
    case CREATESNAPSHOT:
      response = handleCreateSnapshot(path, params, user);
      break;
    case SETXATTR:
      response = handleSetXAttr(path, params, user);
      break;
    case RENAMESNAPSHOT:
      response = handleRenameSnapshot(path, params, user);
      break;
    case REMOVEXATTR:
      response = handleRemoveXAttr(path, params, user);
      break;
    case MKDIRS:
      response = handleMkdirs(path, params, user);
      break;
    case RENAME:
      response = handleRename(path, params, user);
      break;
    case SETOWNER:
      throw new UnsupportedOperationException(getClass().getSimpleName()
          + " doesn't support SETOWNER");
      //response = handleSetOwner(path, params, user);
      //break;
    case SETPERMISSION:
      throw new UnsupportedOperationException(getClass().getSimpleName()
          + " doesn't support SETPERMISSION");
      //response = handleSetPermission(path, params, user);
      //break;
    case SETREPLICATION:
      throw new UnsupportedOperationException(getClass().getSimpleName()
          + " doesn't support SETREPLICATION");
      //response = handleSetReplication(path, params, user);
      //break;
    case SETTIMES:
      throw new UnsupportedOperationException(getClass().getSimpleName()
          + " doesn't support SETTIMES");
      //response = handleSetTimes(path, params, user);
      //break;
    case SETACL:
      response = handleSetACL(path, params, user);
      break;
    case REMOVEACL:
      response = handleRemoveACL(path, user);
      break;
    case MODIFYACLENTRIES:
      response = handleModifyACLEntries(path, params, user);
      break;
    case REMOVEACLENTRIES:
      response = handleRemoveACLEntries(path, params, user);
      break;
    case REMOVEDEFAULTACL:
      response = handleRemoveDefaultACL(path, user);
      break;
    case SETSTORAGEPOLICY:
      response = handleSetStoragePolicy(path, params, user);
      break;
    case SETECPOLICY:
      response = handleSetECPolicy(path, params, user);
      break;
    case SATISFYSTORAGEPOLICY:
      throw new UnsupportedOperationException(getClass().getSimpleName()
          + " doesn't support SATISFYSTORAGEPOLICY");
    default:
      throw new IOException(
        MessageFormat.format("Invalid HTTP PUT operation [{0}]",
                             op.value()));
    }
    return response;
  }

  private Response handleSetECPolicy(String path,
                                     Parameters params,
                                     UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String policyName = params.get(ECPolicyParam.NAME, ECPolicyParam.class);
    FSOperations.FSSetErasureCodingPolicy command
        = new FSOperations.FSSetErasureCodingPolicy(path, policyName);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] to policy [{}]", path, policyName);
    response = Response.ok().build();
    return response;
  }

  private Response handleSetStoragePolicy(String path,
                                          Parameters params,
                                          UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String policyName = params.get(PolicyNameParam.NAME,
        PolicyNameParam.class);
    FSOperations.FSSetStoragePolicy command
        = new FSOperations.FSSetStoragePolicy(path, policyName);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] to policy [{}]", path, policyName);
    response = Response.ok().build();
    return response;
  }

  private Response handleRemoveDefaultACL(String path,
                                          UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSRemoveDefaultAcl command
        = new FSOperations.FSRemoveDefaultAcl(path);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] remove default acl", path);
    response = Response.ok().build();
    return response;
  }

  private Response handleRemoveACLEntries(String path,
                                          Parameters params,
                                          UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String aclSpec = params.get(AclPermissionParam.NAME,
            AclPermissionParam.class);
    FSOperations.FSRemoveAclEntries command
        = new FSOperations.FSRemoveAclEntries(path, aclSpec);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] remove acl entry [{}]", path, aclSpec);
    response = Response.ok().build();
    return response;
  }

  private Response handleModifyACLEntries(String path,
                                          Parameters params,
                                          UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String aclSpec = params.get(AclPermissionParam.NAME,
            AclPermissionParam.class);
    FSOperations.FSModifyAclEntries command
        = new FSOperations.FSModifyAclEntries(path, aclSpec);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] modify acl entry with [{}]", path, aclSpec);
    response = Response.ok().build();
    return response;
  }

  private Response handleRemoveACL(String path, UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSRemoveAcl command
        = new FSOperations.FSRemoveAcl(path);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] removed acl", path);
    response = Response.ok().build();
    return response;
  }

  private Response handleSetACL(String path,
                                Parameters params,
                                UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String aclSpec = params.get(AclPermissionParam.NAME,
            AclPermissionParam.class);
    FSOperations.FSSetAcl command
        = new FSOperations.FSSetAcl(path, aclSpec);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] to acl [{}]", path, aclSpec);
    response = Response.ok().build();
    return response;
  }

  private Response handleRename(String path,
                                Parameters params,
                                UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String toPath = params.get(DestinationParam.NAME, DestinationParam.class);
    FSOperations.FSRename command
        = new FSOperations.FSRename(path, toPath);
    JSONObject json = fsExecute(user, command);
    AUDIT_LOG.info("[{}] to [{}]", path, toPath);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleMkdirs(String path,
                                Parameters params,
                                UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    Short permission = params.get(PermissionParam.NAME,
                                   PermissionParam.class);
    Short unmaskedPermission = params.get(UnmaskedPermissionParam.NAME,
        UnmaskedPermissionParam.class);
    FSOperations.FSMkdirs command =
        new FSOperations.FSMkdirs(path, permission, unmaskedPermission);
    JSONObject json = fsExecute(user, command);
    AUDIT_LOG.info("[{}] permission [{}] unmaskedpermission [{}]",
        path, permission, unmaskedPermission);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleRemoveXAttr(String path,
                                     Parameters params,
                                     UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String xattrName = params.get(XAttrNameParam.NAME, XAttrNameParam.class);
    FSOperations.FSRemoveXAttr command
        = new FSOperations.FSRemoveXAttr(path, xattrName);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] removed xAttr [{}]", path, xattrName);
    response = Response.ok().build();
    return response;
  }

  private Response handleRenameSnapshot(String path,
                                        Parameters params,
                                        UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String oldSnapshotName = params.get(OldSnapshotNameParam.NAME,
        OldSnapshotNameParam.class);
    String snapshotName = params.get(SnapshotNameParam.NAME,
        SnapshotNameParam.class);
    FSOperations.FSRenameSnapshot command
        = new FSOperations.FSRenameSnapshot(path,
        oldSnapshotName,
        snapshotName);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] renamed snapshot [{}] to [{}]", path,
        oldSnapshotName, snapshotName);
    response = Response.ok().build();
    return response;
  }

  private Response handleSetXAttr(String path,
                                  Parameters params,
                                  UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String xattrName = params.get(XAttrNameParam.NAME,
        XAttrNameParam.class);
    String xattrValue = params.get(XAttrValueParam.NAME,
        XAttrValueParam.class);
    EnumSet<XAttrSetFlag> flag = params.get(XAttrSetFlagParam.NAME,
        XAttrSetFlagParam.class);

    FSOperations.FSSetXAttr command = new FSOperations.FSSetXAttr(
        path, xattrName, xattrValue, flag);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] to xAttr [{}]", path, xattrName);
    response = Response.ok().build();
    return response;
  }

  private Response handleCreateSnapshot(String path,
                                        Parameters params,
                                        UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    String snapshotName = params.get(SnapshotNameParam.NAME,
        SnapshotNameParam.class);
    FSOperations.FSCreateSnapshot command
        = new FSOperations.FSCreateSnapshot(path, snapshotName);
    String json = fsExecute(user, command);
    AUDIT_LOG.info("[{}] snapshot created as [{}]", path, snapshotName);
    response = Response.ok(json).type(MediaType.APPLICATION_JSON).build();
    return response;
  }

  private Response handleDisallowSnapshot(String path,
                                          UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSDisallowSnapshot command
        = new FSOperations.FSDisallowSnapshot(path);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] disallowed snapshot", path);
    response = Response.ok().build();
    return response;
  }

  private Response handleAllowSnapshot(String path, UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    FSOperations.FSAllowSnapshot command
        = new FSOperations.FSAllowSnapshot(path);
    fsExecute(user, command);
    AUDIT_LOG.info("[{}] allowed snapshot", path);
    response = Response.ok().build();
    return response;
  }

  private Response handleCreate(InputStream is,
                                UriInfo uriInfo,
                                String path,
                                Parameters params,
                                UserGroupInformation user)
      throws IOException, FileSystemAccessException {
    Response response;
    Boolean hasData = params.get(DataParam.NAME, DataParam.class);
    URI redirectURL = createUploadRedirectionURL(uriInfo,
        HttpFSConstants.Operation.CREATE);
    Boolean noRedirect
        = params.get(NoRedirectParam.NAME, NoRedirectParam.class);
    if (noRedirect) {
      final String js = JsonUtil.toJsonString("Location", redirectURL);
      response = Response.ok(js).type(MediaType.APPLICATION_JSON).build();
    } else if (hasData) {
      Short permission = params.get(PermissionParam.NAME,
                                     PermissionParam.class);
      Short unmaskedPermission = params.get(UnmaskedPermissionParam.NAME,
          UnmaskedPermissionParam.class);
      Boolean override = params.get(OverwriteParam.NAME,
                                    OverwriteParam.class);
      Short replication = params.get(ReplicationParam.NAME,
                                     ReplicationParam.class);
      Long blockSize = params.get(BlockSizeParam.NAME,
                                  BlockSizeParam.class);
      FSOperations.FSCreate command
          = new FSOperations.FSCreate(is, path, permission, override,
            replication, blockSize, unmaskedPermission);
      fsExecute(user, command);
      AUDIT_LOG.info("[{}] permission [{}] override [{}] replication [{}] " +
              "blockSize [{}] unmaskedpermission [{}]",
              new Object[]{path,
                  permission,
                  override,
                  replication,
                  blockSize,
                  unmaskedPermission});
      final String js = JsonUtil.toJsonString(
          "Location", uriInfo.getAbsolutePath());
      response = Response.created(uriInfo.getAbsolutePath())
          .type(MediaType.APPLICATION_JSON).entity(js).build();
    } else {
      response = Response.temporaryRedirect(redirectURL).build();
    }
    return response;
  }

  enum AccessMode {
    READWRITE, WRITEONLY, READONLY;
  }
}
