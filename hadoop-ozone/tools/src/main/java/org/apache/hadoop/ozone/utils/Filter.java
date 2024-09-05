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

package org.apache.hadoop.ozone.utils;

/**
 * Represent class which has info of what operation and value a set of records should be filtered with.
 */
public class Filter {
  private FilterOperator operator;
  private Object value;

  public Filter(FilterOperator operator, Object value) {
    this.operator = operator;
    this.value = value;
  }

  public Filter(String op, Object value) {
    this.operator = getFilterOperator(op);
    this.value = value;
  }

  public FilterOperator getOperator() {
    return operator;
  }

  public void setOperator(FilterOperator operator) {
    this.operator = operator;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public FilterOperator getFilterOperator(String op) {
    if (op.equalsIgnoreCase("equals")) {
      return FilterOperator.EQUALS;
    } else if (op.equalsIgnoreCase("max")) {
      return FilterOperator.MAX;
    } else if (op.equalsIgnoreCase("min")) {
      return FilterOperator.MIN;
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    return operator + ", " + value;
  }

  /**
   * Operation of the filter.
   * */
  public enum FilterOperator {
    EQUALS,
    MAX,
    MIN;
  }
}
