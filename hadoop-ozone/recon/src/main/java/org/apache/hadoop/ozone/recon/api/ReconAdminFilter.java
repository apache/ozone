package org.apache.hadoop.ozone.recon.api;

import java.io.IOException;
import java.util.Collection;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.inject.Singleton;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ReconAdminFilter implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconAdminFilter.class);

  private OzoneConfiguration conf;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    conf = (OzoneConfiguration) filterConfig.getServletContext().getAttribute("OzoneConfiguration");
    LOG.info("ReconAdminFilter init.");
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
    LOG.info("ReconAdminFilter doFilter.");

    HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
    HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;

    final java.security.Principal userPrincipal = httpServletRequest.getUserPrincipal();
    String userPrincipalName = null;
    boolean isAdmin = false;

    if (userPrincipal != null) {
      userPrincipalName = userPrincipal.getName();
      Collection<String> admins =
          conf.getStringCollection(OzoneConfigKeys.OZONE_ADMINISTRATORS);
      if (admins.contains(userPrincipalName)) {
        isAdmin = true;
        filterChain.doFilter(httpServletRequest, httpServletResponse);
      }
    }
    LOG.info("Requester user principal '{}'. Is admin? {}", userPrincipalName
        , isAdmin);

    if (!isAdmin) {
      httpServletResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
    }

  }

  @Override
  public void destroy() {
    LOG.info("ReconAdminFilter destroy.");
  }
}
