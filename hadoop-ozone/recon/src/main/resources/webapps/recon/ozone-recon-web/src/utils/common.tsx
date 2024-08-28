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

import moment from 'moment';
import { notification } from 'antd';

export const getCapacityPercent = (used: number, total: number) => Math.round((used / total) * 100);

export const timeFormat = (time: number) => time > 0 ?
  moment(time).format('lll') : 'NA';

const showErrorNotification = (title: string, description: string) => {
  const args = {
    message: title,
    description,
    duration: 15
  };
  notification.error(args);
};

const showInfoNotification = (title: string, description: string) => {
  const args = {
    message: title,
    description,
    duration: 15
  };
  notification.warn(args);
};

export const showDataFetchError = (error: string) => {
  let title = 'Error while fetching data';
  
  if (error.includes('CanceledError')) return;
  if (error.includes('metadata')) {
    title = 'Metadata Initialization:';
    showInfoNotification(title, error);
    return;
  }
  showErrorNotification(title, error);
};

export const byteToSize = (bytes: number, decimals: number) => {
  if (bytes === 0) {
    return '0 Bytes';
  }
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return isNaN(i) ? `Not Defined` : `${Number.parseFloat((bytes / (k ** i)).toFixed(dm))} ${sizes[i]}`;
};

export const nullAwareLocaleCompare = (a: string, b: string) => {
  if (!a && !b) {
    return 0;
  }

  if (!a) {
    return +1;
  }

  if (!b) {
    return -1;
  }

  return a.localeCompare(b);
};
