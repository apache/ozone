package org.apache.hadoop.ozone.s3.util;

import javax.ws.rs.container.ContainerRequestContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.s3.ClientIpFilter.CLIENT_IP_HEADER;

public class AuditUtils {
  public static Map<String, String> getAuditParameters(
      ContainerRequestContext context) {
    Map<String, String> res = new HashMap<>();
    if (context != null) {
      for (Map.Entry<String, List<String>> entry :
          context.getUriInfo().getPathParameters().entrySet()) {
        res.put(entry.getKey(), entry.getValue().toString());
      }
      for (Map.Entry<String, List<String>> entry :
          context.getUriInfo().getQueryParameters().entrySet()) {
        res.put(entry.getKey(), entry.getValue().toString());
      }
    }
    return res;
  }

  public static String getClientIpAddress(ContainerRequestContext context) {
    return context.getHeaderString(CLIENT_IP_HEADER);
  }
}
