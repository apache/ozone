/*
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

import { useEffect, useRef, useState } from 'react';
import { AUTO_RELOAD_INTERVAL_DEFAULT } from '@/constants/autoReload.constants';

export function useAutoReload(
  refreshFunction: () => void,
  interval: number = AUTO_RELOAD_INTERVAL_DEFAULT
) {
  const intervalRef = useRef<number>(0);
  const [isPolling, setIsPolling] = useState<boolean>(false);
  const refreshFunctionRef = useRef(refreshFunction);
  const lastPollCallRef = useRef<number>(0); // This is used to store the last time poll was called

  // Update the ref when the function changes
  refreshFunctionRef.current = refreshFunction;

  const stopPolling = () => {
    if (intervalRef.current > 0) {
      clearTimeout(intervalRef.current);
      intervalRef.current = 0;
      setIsPolling(false);
    }
  };

  const startPolling = () => {
    stopPolling();
    const poll = () => {
      /**
       * Prevent any extra polling calls within 100ms of the last call,
       * This is done in case at any place multiple API calls are made, for example
       * the useEffect on mount in this component will call the startPolling() function.
       * If this startPolling() function is called elsewhere in a different component then
       * race condition can occur where this gets called in succession multiple times.
       */
      if (Date.now() - lastPollCallRef.current > 100) {
        refreshFunctionRef.current();
        lastPollCallRef.current = Date.now();
      }
      intervalRef.current = window.setTimeout(poll, interval);
    };
    poll();
    setIsPolling(true);
  };

  const handleAutoReloadToggle = (checked: boolean) => {
    sessionStorage.setItem('autoReloadEnabled', JSON.stringify(checked));
    if (checked) {
      startPolling();
    } else {
      stopPolling();
    }
  };

  // Initialize polling on mount if auto-reload is enabled
  useEffect(() => {
    const autoReloadEnabled = sessionStorage.getItem('autoReloadEnabled') !== 'false';
    if (autoReloadEnabled) {
      startPolling();
    }

    return () => {
      stopPolling();
    };
  }, []); // Empty dependency array

  return {
    startPolling,
    stopPolling,
    isPolling,
    handleAutoReloadToggle
  };
}
