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

import { useEffect, useState } from 'react';
import { fetchJmxBean } from './jmx';

export interface JmxBeanState<T> {
  data?: T;
  loading: boolean;
  error?: Error;
}

/**
 * Fetch a single JMX MBean for a section. Requests are de-duplicated by query
 * (see {@link fetchJmxBean}), so multiple sections depending on the same MBean
 * share one network call. Pass a changing `refreshToken` (together with
 * `clearJmxCache()`) to force a refetch.
 */
export function useJmxBean<T>(qry: string, refreshToken = 0): JmxBeanState<T> {
  const [state, setState] = useState<JmxBeanState<T>>({ loading: true });

  useEffect(() => {
    let active = true;
    setState({ loading: true });
    fetchJmxBean<T>(qry)
      .then((data) => {
        if (active) {
          setState({ data, loading: false });
        }
      })
      .catch((error: Error) => {
        if (active) {
          setState({ loading: false, error });
        }
      });
    return () => {
      active = false;
    };
  }, [qry, refreshToken]);

  return state;
}
