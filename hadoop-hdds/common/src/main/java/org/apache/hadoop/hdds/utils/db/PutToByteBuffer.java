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

package org.apache.hadoop.hdds.utils.db;

import java.nio.ByteBuffer;
import org.apache.ratis.util.function.CheckedFunction;

/**
 * A function puts data from a source to the {@link ByteBuffer}
 * specified in the parameter.
 * The source may or may not be available.
 * This function must return the required size (possibly 0)
 * if the source is available; otherwise, return null.
 * When the {@link ByteBuffer}'s capacity is smaller than the required size,
 * partial data may be put to the {@link ByteBuffer}.
 *
 * @param <E> The exception type this function may throw.
 */
@FunctionalInterface
interface PutToByteBuffer<E extends Exception>
    extends CheckedFunction<ByteBuffer, Integer, E> {
}
