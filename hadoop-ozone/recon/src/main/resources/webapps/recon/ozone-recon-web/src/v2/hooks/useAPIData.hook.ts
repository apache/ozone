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
  success: boolean; // For non-GET requests to indicate successful completion
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
    initialFetch = method === 'GET', // Only auto-fetch for GET requests
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
  
  // Store stable references
  const urlRef = useRef(url);
  const methodRef = useRef(method);
  const retryAttemptsRef = useRef(retryAttempts);
  const retryDelayRef = useRef(retryDelay);
  const onErrorRef = useRef(onError);
  const onSuccessRef = useRef(onSuccess);

  // Update refs when props change
  useEffect(() => {
    urlRef.current = url;
  }, [url]);

  useEffect(() => {
    methodRef.current = method;
  }, [method]);

  useEffect(() => {
    retryAttemptsRef.current = retryAttempts;
  }, [retryAttempts]);

  useEffect(() => {
    retryDelayRef.current = retryDelay;
  }, [retryDelay]);

  useEffect(() => {
    onErrorRef.current = onError;
  }, [onError]);

  useEffect(() => {
    onSuccessRef.current = onSuccess;
  }, [onSuccess]);

  const executeRequest = async (requestData?: any, isRetry = false) => {
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
        url: urlRef.current,
        method: methodRef.current,
        signal: controllerRef.current.signal,
      };

      // Add data for non-GET requests
      if (methodRef.current !== 'GET' && requestData !== undefined) {
        config.data = requestData;
      }

      // Add query parameters for GET requests if data is provided as params
      if (methodRef.current === 'GET' && requestData !== undefined) {
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

      if (onSuccessRef.current) {
        onSuccessRef.current(response.data);
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
                          `${methodRef.current} request failed with status: ${error.response?.status || 'unknown'}`;

      // Clear any existing retry timeout
      if (retryTimeoutRef.current) {
        clearTimeout(retryTimeoutRef.current);
      }

      // Retry logic for network errors and 5xx errors
      if (retryCountRef.current < retryAttemptsRef.current && 
          (!error.response?.status || error.response?.status >= 500)) {
        retryCountRef.current++;
        retryTimeoutRef.current = setTimeout(() => {
          executeRequest(requestData, true);
        }, retryDelayRef.current * retryCountRef.current);
        return Promise.reject(error);
      }

      if (onErrorRef.current) {
        onErrorRef.current(error);
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

  // Initial fetch for GET requests only
  useEffect(() => {
    if (initialFetch && methodRef.current === 'GET') {
      executeRequest();
    }

    // Cleanup retry timeout on unmount
    return () => {
      if (retryTimeoutRef.current) {
        clearTimeout(retryTimeoutRef.current);
      }
    };
  }, []); // Empty dependency array

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (controllerRef.current) {
        controllerRef.current.abort('Component unmounted');
      }
      if (retryTimeoutRef.current) {
        clearTimeout(retryTimeoutRef.current);
      }
    };
  }, []);

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
