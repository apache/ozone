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

package org.apache.hadoop.ozone.s3;

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_DOMAIN_NAME;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter used to convert virtual host style pattern to path style pattern.
 */

@Provider
@PreMatching
@Priority(VirtualHostStyleFilter.PRIORITY)
public class VirtualHostStyleFilter implements ContainerRequestFilter {

  public static final int PRIORITY = AuthorizationFilter.PRIORITY +
      S3GatewayHttpServer.FILTER_PRIORITY_DO_AFTER;

  private static final Logger LOG = LoggerFactory.getLogger(
      VirtualHostStyleFilter.class);

  @Inject
  private OzoneConfiguration conf;

  private String[] domains;

  @Override
  public void filter(ContainerRequestContext requestContext) throws
      IOException {
    domains = conf.getTrimmedStrings(OZONE_S3G_DOMAIN_NAME);

    if (domains.length == 0) {
      // domains is not configured, might be it is path style.
      // So, do not continue further, just return.
      return;
    }
    //Get the value of the host
    String host = requestContext.getHeaderString(HttpHeaders.HOST);
    host = checkHostWithoutPort(host);
    String domain = getDomainName(host);

    if (domain == null) {
      throw getException("Invalid S3 Gateway request {" + requestContext
          .getUriInfo().getRequestUri().toString() + " }: No matching domain " +
          "{" + Arrays.toString(domains) + "} for the host {" + host  + "}");
    }

    LOG.debug("Http header host name is {}", host);
    LOG.debug("Domain name matched is {}", domain);

    //Check if we have a Virtual Host style request, host length greater than
    // address length means it is virtual host style, we need to convert to
    // path style.
    if (host.length() > domain.length()) {
      String bucketName = host.substring(0, host.length() - domain.length());

      if (!bucketName.endsWith(".")) {
        //Checking this as the virtual host style pattern is http://bucket.host/
        throw getException("Invalid S3 Gateway request {" + requestContext
            .getUriInfo().getRequestUri().toString() + "}:" + " Host: {" + host
            + " is in invalid format");
      } else {
        bucketName = bucketName.substring(0, bucketName.length() - 1);
      }
      LOG.debug("Bucket name is {}", bucketName);

      URI baseURI = requestContext.getUriInfo().getBaseUri();
      String currentPath = requestContext.getUriInfo().getPath();
      String newPath = bucketName;
      MultivaluedMap<String, String> queryParams = requestContext.getUriInfo()
          .getQueryParameters();
      UriBuilder requestAddrBuilder = UriBuilder.fromUri(baseURI).path(newPath);
      if (currentPath != null) {
        requestAddrBuilder.path(currentPath);
      }
      queryParams.forEach((k, v) -> requestAddrBuilder.queryParam(k,
          v.toArray()));
      URI requestAddr = requestAddrBuilder.build();
      requestContext.setRequestUri(baseURI, requestAddr);
    }
  }

  private InvalidRequestException getException(String message) {
    return new InvalidRequestException(message);
  }

  @VisibleForTesting
  public void setConfiguration(OzoneConfiguration config) {
    this.conf = config;
  }

  /**
   * This method finds the longest match with the domain name.
   * @param host
   * @return domain name matched with the host. if none of them are matching,
   * return null.
   */
  private String getDomainName(String host) {
    String match = null;
    int length = 0;
    for (String domainVal : domains) {
      if (host.endsWith(domainVal)) {
        int len = domainVal.length();
        if (len > length) {
          length = len;
          match = domainVal;
        }
      }
    }
    return match;
  }

  private String checkHostWithoutPort(String host) {
    if (host.contains(":")) {
      return host.substring(0, host.lastIndexOf(":"));
    } else {
      return host;
    }
  }
}
