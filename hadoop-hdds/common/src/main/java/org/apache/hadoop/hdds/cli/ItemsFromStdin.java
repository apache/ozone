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

package org.apache.hadoop.hdds.cli;

import static java.util.Collections.unmodifiableList;

import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

/** Parameter for specifying list of items, reading from stdin if "-" is given as first item. */
public abstract class ItemsFromStdin implements Iterable<String> {

  protected static final String FORMAT_DESCRIPTION =
      ": one or more, separated by spaces. To read from stdin, specify '-' and supply one item per line.";

  private List<String> items = new ArrayList<>();

  protected void setItems(List<String> arguments) {
    items = readItemsFromStdinIfNeeded(arguments);
  }

  public List<String> getItems() {
    return unmodifiableList(items);
  }

  @Nonnull
  @Override
  public Iterator<String> iterator() {
    return items.iterator();
  }

  public int size() {
    return items.size();
  }

  private static List<String> readItemsFromStdinIfNeeded(List<String> parameters) {
    if (parameters.isEmpty() || !"-".equals(parameters.iterator().next())) {
      return parameters;
    }

    List<String> items = new ArrayList<>();
    Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
    while (scanner.hasNextLine()) {
      items.add(scanner.nextLine().trim());
    }
    return items;
  }
}
