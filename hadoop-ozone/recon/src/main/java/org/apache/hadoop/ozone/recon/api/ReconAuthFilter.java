package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX;
import static org.apache.hadoop.security.AuthenticationFilterInitializer.getFilterConfigMap;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.eclipse.jetty.servlet.FilterHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReconAuthFilter implements Filter {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconAuthFilter.class);

  private OzoneConfiguration conf;
  private AuthenticationFilter hadoopAuthFilter;


  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    conf = (OzoneConfiguration) filterConfig.getServletContext().getAttribute("OzoneConfiguration");
    LOG.info("ReconAuthFilter init.");
    hadoopAuthFilter = new AuthenticationFilter();

    Map<String, String> parameters = getFilterConfigMap(conf,
        OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX);
    FilterHolder filterHolder = getFilterHolder("authentication",
        AuthenticationFilter.class.getName(),
        parameters);
    hadoopAuthFilter.init(new FilterConfig() {
      @Override
      public String getFilterName() {
        return filterHolder.getName();
      }

      @Override
      public ServletContext getServletContext() {
        return filterConfig.getServletContext();
      }

      @Override
      public String getInitParameter(String s) {
        return filterHolder.getInitParameter(s);
      }

      @Override
      public Enumeration<String> getInitParameterNames() {
        return filterHolder.getInitParameterNames();
      }
    });
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
    LOG.info("AuthFilter doFilter.");
    hadoopAuthFilter.doFilter(servletRequest, servletResponse, filterChain);
  }

  private static FilterHolder getFilterHolder(String name, String classname,
                                              Map<String, String> parameters) {
    FilterHolder holder = new FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    if (parameters != null) {
      holder.setInitParameters(parameters);
    }
    return holder;
  }

  @Override
  public void destroy() {
    LOG.info("AuthFilter destroy.");
  }
}
