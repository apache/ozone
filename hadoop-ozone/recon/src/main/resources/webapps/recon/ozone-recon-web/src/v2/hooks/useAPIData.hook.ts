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

import { useState, useEffect, useRef } from 'react';
import axios, { AxiosError, AxiosRequestConfig } from 'axios';

export type HttpMethod = 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';

export interface ApiState<T> {
  data: T;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
  success: boolean;
}

export interface UseApiDataOptions {
  method?: HttpMethod;
  retryAttempts?: number;
  retryDelay?: number;
  initialFetch?: boolean;
  onError?: (error: AxiosError | string | unknown) => void;
  onSuccess?: (data: any) => void;
}

export function useApiData<T>(
  url: string,
  defaultValue: T,
  options: UseApiDataOptions = {}
): ApiState<T> & {
  execute: (data?: any) => Promise<any>;
  refetch: () => Promise<any>;
  clearError: () => void;
  reset: () => void;
} {
  const {
    method = 'GET',
    retryAttempts = 3,
    retryDelay = 1000,
    initialFetch = method === 'GET',
    onError,
    onSuccess
  } = options;

  const [state, setState] = useState<ApiState<T>>({
    data: defaultValue,
    loading: initialFetch,
    error: null,
    lastUpdated: null,
    success: false
  });

  const controllerRef = useRef<AbortController>();
  const retryCountRef = useRef(0);
  const retryTimeoutRef = useRef<NodeJS.Timeout>();
  const mountedRef = useRef(false);

  const executeRequest = async (requestData?: any, isRetry = false) => {
    // Don't make requests if URL is empty or falsy
    if (!url || url.trim() === '') {
      return Promise.reject(new Error('URL is required'));
    }

    if (!isRetry) {
      setState(prev => ({ ...prev, loading: true, error: null, success: false }));
      retryCountRef.current = 0;
    }

    // Cancel previous request
    if (controllerRef.current) {
      controllerRef.current.abort('New request initiated');
    }

    // Create new AbortController
    controllerRef.current = new AbortController();

    try {
      const config: AxiosRequestConfig = {
        url,
        method,
        signal: controllerRef.current.signal,
      };

      // Add data for non-GET requests
      if (method !== 'GET' && requestData !== undefined) {
        config.data = requestData;
      }

      // Add query parameters for GET requests if data is provided as params
      if (method === 'GET' && requestData !== undefined) {
        config.params = requestData;
      }

      const response = await axios(config);
      
      setState({
        data: response.data,
        loading: false,
        error: null,
        lastUpdated: Date.now(),
        success: true
      });

      if (onSuccess) {
        onSuccess(response.data);
      }

      retryCountRef.current = 0;
      return response;
    } catch (error: any) {
      if (error.name === 'CanceledError' || error.name === 'AbortError') {
        return Promise.reject(error);
      }

      const errorMessage = error.response?.data?.message ||
                          error.response?.statusText ||
                          error.message ||
                          `${method} request failed with status: ${error.response?.status || 'unknown'}`;

      // Clear any existing retry timeout
      if (retryTimeoutRef.current) {
        clearTimeout(retryTimeoutRef.current);
      }

      // Retry logic for network errors and 5xx errors
      if (retryCountRef.current < retryAttempts && 
          (!error.response?.status || error.response?.status >= 500)) {
        retryCountRef.current++;
        retryTimeoutRef.current = setTimeout(() => {
          executeRequest(requestData, true);
        }, retryDelay * retryCountRef.current);
        return Promise.reject(error);
      }

      if (onError) {
        onError(error);
      }

      setState({
        data: defaultValue,
        loading: false,
        error: errorMessage,
        lastUpdated: Date.now(),
        success: false
      });

      return Promise.reject(error);
    }
  };

  const execute = (data?: any) => {
    return executeRequest(data);
  };

  const refetch = () => {
    return executeRequest();
  };

  const clearError = () => {
    setState(prev => ({ ...prev, error: null }));
  };

  const reset = () => {
    setState({
      data: defaultValue,
      loading: false,
      error: null,
      lastUpdated: null,
      success: false
    });
  };

  // Handle initial fetch, URL changes, and cleanup
  useEffect(() => {
    // Don't make requests if URL is empty or falsy
    if (!url || url.trim() === '') {
      return;
    }

    if (!mountedRef.current) {
      // Initial mount - this is required since we might have a situation where
      // the component is mounted but initial fetch is not enabled, hence we need to separate out
      // by checking if the component is mounted or just the URL has changed.
      mountedRef.current = true;
      if (initialFetch && method === 'GET') {
        executeRequest();
      }
    } else {
      // URL changed - refetch for GET requests
      if (method === 'GET') {
        executeRequest();
      }
    }

    // Cleanup on unmount
    return () => {
      if (controllerRef.current) {
        controllerRef.current.abort();
      }
      if (retryTimeoutRef.current) {
        clearTimeout(retryTimeoutRef.current);
      }
    };
  }, [url]); // eslint-disable-line react-hooks/exhaustive-deps

  return {
    ...state,
    execute,
    refetch,
    clearError,
    reset
  };
}

// Utility function for manual single requests (for dynamic/on-demand usage)
export async function fetchData<T>(
  url: string, 
  method: HttpMethod = 'GET', 
  data?: any
): Promise<T> {
  // Don't make requests if URL is empty or falsy
  if (!url || url.trim() === '') {
    return Promise.reject(new Error('URL is required'));
  }

  const controller = new AbortController();
  
  const config: AxiosRequestConfig = {
    url,
    method,
    signal: controller.signal,
  };

  if (method !== 'GET' && data !== undefined) {
    config.data = data;
  }

  if (method === 'GET' && data !== undefined) {
    config.params = data;
  }

  const response = await axios(config);
  return response.data;
}
