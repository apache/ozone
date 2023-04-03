package org.apache.hadoop.hdds.server.http;

import java.util.Map;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;

public final class ServletElementsFactory {
  private ServletElementsFactory() {
    throw new UnsupportedOperationException("This is utility class and cannot be instantiated");
  }

  public static FilterMapping createFilterMapping(String name, String[] urls) {
    FilterMapping filterMapping = new FilterMapping();
    filterMapping.setPathSpecs(urls);
    filterMapping.setDispatches(FilterMapping.ALL);
    filterMapping.setFilterName(name);
    return filterMapping;
  }

  public static FilterHolder createFilterHolder(String name, String classname, Map<String, String> parameters) {
    FilterHolder holder = new FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    if (parameters != null) {
      holder.setInitParameters(parameters);
    }
    return holder;
  }
}
