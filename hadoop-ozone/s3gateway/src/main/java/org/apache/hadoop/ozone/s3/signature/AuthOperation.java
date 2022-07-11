package org.apache.hadoop.ozone.s3.signature;

import org.apache.hadoop.ozone.audit.AuditAction;

import javax.ws.rs.container.ContainerRequestContext;

public class AuthOperation implements AuditAction {
  private final String path;
  private final String method;

  public AuthOperation(final String path, final String method) {
    this.path = path;
    this.method = method;
  }

  public static AuthOperation fromContext(ContainerRequestContext context) {
    return new AuthOperation(context.getUriInfo().getPath(),
        context.getMethod());
  }

  @Override
  public String getAction() {
    return String.format("%s %s", method, path);
  }
}
