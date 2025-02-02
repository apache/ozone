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

package org.apache.hadoop.hdds.server.http;

import java.util.Map;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;

/**
 * Factory class which helps to create different types of servlet elements.
 */
public final class ServletElementsFactory {
  private ServletElementsFactory() {
    throw new UnsupportedOperationException(
        "This is utility class and cannot be instantiated");
  }

  public static FilterMapping createFilterMapping(String mappingName,
      String[] urls) {
    FilterMapping filterMapping = new FilterMapping();
    filterMapping.setPathSpecs(urls);
    filterMapping.setDispatches(FilterMapping.ALL);
    filterMapping.setFilterName(mappingName);
    return filterMapping;
  }

  public static FilterHolder createFilterHolder(String filterName,
      String classname, Map<String, String> parameters) {
    FilterHolder holder = new FilterHolder();
    holder.setName(filterName);
    holder.setClassName(classname);
    if (parameters != null) {
      holder.setInitParameters(parameters);
    }
    return holder;
  }
}
