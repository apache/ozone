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

package org.apache.hadoop.hdds.utils.db.managed;

import org.rocksdb.TypeUtil;

/**
 * Utility class that provides methods to manage and handle specific aspects of
 * managed types and interactions with RocksDB objects. It extends TypeUtil,
 * inheriting its utilities while offering specialized functionality for
 * managed database components.
 *
 * This class is part of the framework designed to ensure proper handling and
 * lifecycle management of RocksDB objects, reducing the risks of native
 * resource leaks by tracking and enforcing explicit closure of objects.
 */
public class ManagedTypeUtil extends TypeUtil {
}
