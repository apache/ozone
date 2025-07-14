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

package org.apache.hadoop.ozone.utils;

import java.util.Map;

/**
 * Represent class which has info of what operation and value a set of records should be filtered with.
 */
public class Filter {
  private FilterOperator operator;
  private Object value;
  private Map<String, Filter> nextLevel = null;

  public Filter() {
    this.operator = null;
    this.value = null;
  }

  public Filter(FilterOperator operator, Object value) {
    this.operator = operator;
    this.value = value;
  }

  public Filter(String op, Object value) {
    this.operator = getFilterOperator(op);
    this.value = value;
  }

  public Filter(FilterOperator operator, Object value, Map<String, Filter> next) {
    this.operator = operator;
    this.value = value;
    this.nextLevel = next;
  }

  public Filter(String op, Object value, Map<String, Filter> next) {
    this.operator = getFilterOperator(op);
    this.value = value;
    this.nextLevel = next;
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

  public Map<String, Filter> getNextLevel() {
    return nextLevel;
  }

  public void setNextLevel(Map<String, Filter> nextLevel) {
    this.nextLevel = nextLevel;
  }

  public FilterOperator getFilterOperator(String op) {
    if (op.equalsIgnoreCase("equals")) {
      return FilterOperator.EQUALS;
    } else if (op.equalsIgnoreCase("GREATER")) {
      return FilterOperator.GREATER;
    } else if (op.equalsIgnoreCase("LESSER")) {
      return FilterOperator.LESSER;
    } else if (op.equalsIgnoreCase("REGEX")) {
      return FilterOperator.REGEX;
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    return "(" + operator + "," + value + "," + nextLevel + ")";
  }

  /**
   * Operation of the filter.
   */
  public enum FilterOperator {
    EQUALS,
    LESSER,
    GREATER,
    REGEX;
  }
}
