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

import axios, { AxiosResponse } from 'axios';

export const AxiosGetHelper = (
  url: string,
  controller: AbortController | undefined,
  message: string = '',
  params: any = {},
): { request: Promise<AxiosResponse<any, any>>; controller: AbortController } => {

  controller && controller.abort(message);
  controller = new AbortController(); // generate new AbortController for the upcoming request

  return {
    request: axios.get(url, { signal: controller.signal, params: params }),
    controller: controller
  }
}

export const AxiosPutHelper = (
  url: string,
  data: any = {},
  controller: AbortController | undefined,
  message: string = '',  //optional
): { request: Promise<AxiosResponse<any, any>>; controller: AbortController } => {
  controller && controller.abort(message);
  controller = new AbortController(); // generate new AbortController for the upcoming request
  return {
    request: axios.put(url, data, { signal: controller.signal }),
    controller: controller
  }
}

export const PromiseAllSettledGetHelper = (
  urls: string[],
  controller: AbortController | undefined,
  message: string = ''
): { requests: Promise<PromiseSettledResult<AxiosResponse<any, any>>[]>; controller: AbortController } => {

  controller && controller.abort(message);
  controller = new AbortController(); // generate new AbortController for the upcoming request

  //create axios get requests
  let axiosGetRequests: Promise<AxiosResponse<any, any>>[] = [];
  urls.forEach((url) => {
    axiosGetRequests.push(axios.get(url, { signal: controller.signal }))
  });

  return {
    requests: Promise.allSettled(axiosGetRequests),
    controller: controller
  }
}

export const cancelRequests = (cancelSignal: AbortController[]) => {
  cancelSignal.forEach((signal) => {
    signal && signal.abort();
  });
}