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
import { CanceledError } from 'axios';

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

/**
 * This method converts a number to the respective closest value in words
 * Ex:
 *    (num: 1234, digits: 1) = 1.2k
 *    (num: 1234, digits: 2) = 1.23k
 * 
 * In case it reaches 1e18 i.e more than 18 digits present, it will give the nearest value
 * Ex (if it was going upto a thousand, i.e K is the largest prefix it can provide)
 *    (num: 12300000, digits: 1) = 12300k
 * 
 * Similarly for something exceeding billions, it will give to the closest billionth value
 * @param num: The number to convert
 * @param digits: The number of digits after decimal 
 */
export function numberToWord(num: number, digits: number) {
  const matcherTable = [
    { value: 1, symbol: "" },
    { value: 1e3, symbol: "K" },
    { value: 1e6, symbol: "M" },
    { value: 1e9, symbol: "B" }
  ]

  const regexp = /\.0+$|(?<=\.[0-9]*[1-9])0+$/;
  const item = matcherTable.slice().reverse().find(item => num >= item.value);
  return item ? (num / item.value).toFixed(digits).replace(regexp, "").concat(item.symbol) : "0";
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
      if (err.reason.toString().includes("CanceledError")) {
        throw new CanceledError('canceled', "ERR_CANCELED");
      }
      else {
        const reqMethod = err.reason.config.method;
        const reqURL = err.reason.config.url
        showDataFetchError(
          `Failed to ${reqMethod} URL ${reqURL}\n${err.reason.toString()}`
        );
      }
    })
  }
}