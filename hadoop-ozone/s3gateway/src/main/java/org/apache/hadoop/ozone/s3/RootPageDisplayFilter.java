package org.apache.hadoop.ozone.s3;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * This redirect helps to show and info page in case the endpoint is opened
 * from the browser.
 */
public class RootPageDisplayFilter implements Filter {

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {

  }

  @Override
  public void doFilter(ServletRequest servletRequest,
      ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
    String httpMethod = httpRequest.getMethod();
    String uri = httpRequest.getRequestURI();
    String authorizationHeader = httpRequest.getHeader("Authorization");
    if (httpMethod.equalsIgnoreCase("GET") && authorizationHeader == null && uri
        .equals("/")) {
      ((HttpServletResponse) servletResponse).sendRedirect("/static/");
    } else {
      filterChain.doFilter(httpRequest, servletResponse);
    }
  }

  @Override
  public void destroy() {

  }
}
