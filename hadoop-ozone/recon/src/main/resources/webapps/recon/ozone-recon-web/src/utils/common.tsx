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
import axios, { CanceledError, AxiosError } from 'axios';

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

export const showInfoNotification = (title: string, description: string) => {
  const args = {
    message: title,
    description,
    duration: 15
  };
  notification.warn(args);
};

export const showDataFetchError = (error: string | AxiosError | unknown) => {
  let title = 'Error while fetching data';
  let errorMessage = '';
  
  // Handle AxiosError instances
  if (axios.isAxiosError(error)) {
    // Don't show notifications for canceled requests
    if (error.code === 'ERR_CANCELED' || error.name === 'CanceledError') {
      return;
    }
    
    if (error.response) {
      // Server responded with error status
      errorMessage = `Server Error (${error.response.status}): ${error.response.statusText}`;
      if (error.response.data && typeof error.response.data === 'string') {
        errorMessage += ` - ${error.response.data}`;
      }
    } else if (error.request) {
      // Request was made but no response received
      errorMessage = 'Network Error: No response received from server';
    } else {
      // Something else happened
      errorMessage = error.message || 'Unknown error occurred';
    }
  } else {
    errorMessage = error as string;
    
    if (errorMessage.includes('CanceledError')) return;
    if (errorMessage.includes('metadata')) {
      title = 'Metadata Initialization:';
      showInfoNotification(title, errorMessage);
      return;
    }
  }
  
  showErrorNotification(title, errorMessage);
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

/**
 * The function transforms the provided number to a comma separated value
 * Ex: 1000 will be 1,000
 * @param num The number to convert
 * @returns The number separated at every thousandth place by commas
 */
export function numberWithCommas(num: number) {
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

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

export function removeDuplicatesAndMerge<T>(origArr: T[], updateArr: T[], mergeKey: string): T[] {
  return Array.from([...origArr, ...updateArr].reduce(
    (accumulator, curr) => accumulator.set(curr[mergeKey as keyof T], curr),
    new Map
  ).values());
}

export const checkResponseError = (responses: Awaited<Promise<any>>[]) => {
  const responseError = responses.filter(
    (resp) => resp.status === 'rejected'
  );

  if (responseError.length !== 0) {
    responseError.forEach((err) => {
      if (err.reason instanceof CanceledError || err.reason.code === 'ERR_CANCELED') {
        throw new CanceledError('canceled', "ERR_CANCELED");
      }
      else {
        const reqMethod = err.reason.config?.method || 'unknown';
        const reqURL = err.reason.config?.url || 'unknown URL';
        showDataFetchError(
          `Failed to ${reqMethod} URL ${reqURL}\n${err.reason.toString()}`
        );
      }
    })
  }
}