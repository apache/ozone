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

import { AUTO_RELOAD_INTERVAL_DEFAULT } from '@/constants/autoReload.constants';

class AutoReloadHelper {
  loadData: () => void;
  interval = 0;

  constructor(loadData: () => void) {
    this.loadData = loadData;
  }

  initPolling = () => {
    this.loadData();
    this.interval = window.setTimeout(this.initPolling, AUTO_RELOAD_INTERVAL_DEFAULT);
  };

  startPolling = () => {
    this.stopPolling();
    this.interval = window.setTimeout(this.initPolling, AUTO_RELOAD_INTERVAL_DEFAULT);
  };

  stopPolling = () => {
    if (this.interval > 0) {
      clearTimeout(this.interval);
    }
  };

  handleAutoReloadToggle = (checked: boolean) => {
    sessionStorage.setItem('autoReloadEnabled', JSON.stringify(checked));
    if (checked) {
      this.startPolling();
    } else {
      this.stopPolling();
    }
  };
}

export { AutoReloadHelper };
