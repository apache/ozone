import { useState, useEffect, useRef } from 'react';
import { AxiosGetHelper } from '@/utils/axiosRequestHelper';

export interface ApiState<T> {
  data: T;
  loading: boolean;
  error: string | null;
  lastUpdated: number | null;
}

export interface UseApiDataOptions {
  retryAttempts?: number;
  retryDelay?: number;
  initialFetch?: boolean;
}

export function useApiData<T>(
  url: string,
  defaultValue: T,
  options: UseApiDataOptions = {}
): ApiState<T> & {
  refetch: () => void;
  clearError: () => void;
} {
  const {
    retryAttempts = 3,
    retryDelay = 1000,
    initialFetch = true
  } = options;

  const [state, setState] = useState<ApiState<T>>({
    data: defaultValue,
    loading: initialFetch,
    error: null,
    lastUpdated: null
  });

  const controllerRef = useRef<AbortController>();
  const retryCountRef = useRef(0);
  const retryTimeoutRef = useRef<NodeJS.Timeout>();
  
  // Store stable references
  const urlRef = useRef(url);
  const retryAttemptsRef = useRef(retryAttempts);
  const retryDelayRef = useRef(retryDelay);

  // Update refs when props change
  useEffect(() => {
    urlRef.current = url;
  }, [url]);

  useEffect(() => {
    retryAttemptsRef.current = retryAttempts;
  }, [retryAttempts]);

  useEffect(() => {
    retryDelayRef.current = retryDelay;
  }, [retryDelay]);

  const fetchData = async (isRetry = false) => {
    if (!isRetry) {
      setState(prev => ({ ...prev, loading: true, error: null }));
      retryCountRef.current = 0;
    }

    try {
      const { request, controller } = AxiosGetHelper(
        urlRef.current,
        controllerRef.current,
        'Request cancelled due to component unmount or new request'
      );
      controllerRef.current = controller;

      const response = await request;
      
      setState({
        data: response.data,
        loading: false,
        error: null,
        lastUpdated: Date.now()
      });

      retryCountRef.current = 0;
    } catch (error: any) {
      if (error.name === 'CanceledError') {
        return;
      }

      const errorMessage = error.response?.data?.message || 
                          error.message || 
                          'An unexpected error occurred';

      // Clear any existing retry timeout
      if (retryTimeoutRef.current) {
        clearTimeout(retryTimeoutRef.current);
      }

      // Retry logic for network errors and 5xx errors
      if (retryCountRef.current < retryAttemptsRef.current && 
          (!error.response?.status || error.response?.status >= 500)) {
        retryCountRef.current++;
        retryTimeoutRef.current = setTimeout(() => {
          fetchData(true);
        }, retryDelayRef.current * retryCountRef.current);
        return;
      }

      setState({
        data: defaultValue,
        loading: false,
        error: errorMessage,
        lastUpdated: Date.now()
      });
    }
  };

  const refetch = () => {
    fetchData();
  };

  const clearError = () => {
    setState(prev => ({ ...prev, error: null }));
  };

  // Initial fetch only
  useEffect(() => {
    if (initialFetch) {
      fetchData();
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
    refetch,
    clearError
  };
}
