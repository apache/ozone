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

import { useState, useRef, useEffect, useCallback } from 'react';
import { AxiosPostHelper } from '@/utils/axiosRequestHelper';
import { ChatMessage, ChatbotChatRequest, ChatbotChatResponse, ChatbotErrorResponse } from '@/v2/types/chatbot.types';
import {
  CHATBOT_ENDPOINTS,
  DEFAULT_MODEL_SENTINEL,
  getLlmProviderFailureMessage,
  isMaskedLlmProcessingError,
  RECON_LOGS_HINT
} from '@/v2/constants/chatbot.constants';
import axios, { AxiosError } from 'axios';

export const useChat = () => {
  const [messages, setMessages] = useState<ChatMessage[]>(() => {
    const saved = sessionStorage.getItem('recon_ai_messages');
    if (saved) {
      try {
        return JSON.parse(saved);
      } catch (e) {
        console.error('Failed to parse saved chat messages', e);
      }
    }
    return [];
  });
  const [isInFlight, setIsInFlight] = useState(false);
  const isInFlightRef = useRef(false);
  const [elapsedSeconds, setElapsedSeconds] = useState(0);
  const [errorBubble, setErrorBubble] = useState<string | null>(null);
  const [currentQuery, setCurrentQuery] = useState('');

  const controllerRef = useRef<AbortController | undefined>();
  const timerRef = useRef<NodeJS.Timeout | undefined>();

  useEffect(() => {
    sessionStorage.setItem('recon_ai_messages', JSON.stringify(messages));
  }, [messages]);

  const startTimer = useCallback(() => {
    setElapsedSeconds(0);
    timerRef.current = setInterval(() => {
      setElapsedSeconds(prev => prev + 1);
    }, 1000);
  }, []);

  const stopTimer = useCallback(() => {
    if (timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = undefined;
    }
  }, []);

  useEffect(() => {
    return () => {
      stopTimer();
      if (controllerRef.current) {
        controllerRef.current.abort();
      }
    };
  }, [stopTimer]);

  const cancelRequest = useCallback(() => {
    if (controllerRef.current) {
      controllerRef.current.abort('User cancelled the request');
      controllerRef.current = undefined;
    }
    setIsInFlight(false);
    isInFlightRef.current = false;
    stopTimer();
  }, [stopTimer]);

  const sendMessage = useCallback(async (query: string, model?: string, provider?: string) => {
    if (!query.trim() || isInFlightRef.current) return;

    setErrorBubble(null);
    setIsInFlight(true);
    isInFlightRef.current = true;
    setCurrentQuery(query);
    startTimer();

    const userMessage: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'user',
      text: query,
      timestamp: Date.now(),
      model,
      provider
    };

    setMessages(prev => {
      // If the last message is a user message with the same text, don't append a new one
      if (prev.length > 0) {
        const lastMsg = prev[prev.length - 1];
        if (lastMsg.role === 'user' && lastMsg.text === query) {
          return prev;
        }
      }
      return [...prev, userMessage];
    });

    const requestBody: ChatbotChatRequest = {
      query: query.trim()
    };

    if (provider && provider !== DEFAULT_MODEL_SENTINEL) {
      requestBody.provider = provider;
    }

    if (model && model !== DEFAULT_MODEL_SENTINEL) {
      requestBody.model = model;
    }

    const { request, controller } = AxiosPostHelper(
      CHATBOT_ENDPOINTS.CHAT,
      requestBody,
      controllerRef.current,
      'New request initiated'
    );
    controllerRef.current = controller;

    try {
      const response = await request;
      const data = response.data as ChatbotChatResponse;
      
      const assistantMessage: ChatMessage = {
        id: crypto.randomUUID(),
        role: 'assistant',
        text: data.response,
        timestamp: Date.now()
      };
      
      setMessages(prev => [...prev, assistantMessage]);
      setCurrentQuery('');
    } catch (error) {
      if (axios.isCancel(error)) {
        // Request was cancelled, do nothing
      } else {
        const axiosError = error as AxiosError<ChatbotErrorResponse>;
        const status = axiosError.response?.status;
        const errorText = axiosError.response?.data?.error || axiosError.message;
        
        let displayError = errorText;
        if (status === 503) {
          // Could be disabled or busy
          displayError = errorText || 'Service is currently busy. Please retry in a moment.';
        } else if (status === 504) {
          displayError = errorText || 'Request timed out. The query took too long to process. Please try a different or faster model.';
        } else if (status === 500) {
          if (isMaskedLlmProcessingError(errorText)) {
            displayError = getLlmProviderFailureMessage(provider, model);
          } else {
            displayError = `${errorText}\n\n${RECON_LOGS_HINT}`;
          }
        } else if (status === 400) {
          displayError = errorText || 'Invalid request. Please check your query.';
        }

        setErrorBubble(displayError);
      }
    } finally {
      setIsInFlight(false);
      isInFlightRef.current = false;
      stopTimer();
    }
  }, [startTimer, stopTimer]);

  const clearMessages = useCallback(() => {
    setMessages([]);
    setErrorBubble(null);
    sessionStorage.removeItem('recon_ai_messages');
  }, []);

  return {
    messages,
    isInFlight,
    elapsedSeconds,
    errorBubble,
    currentQuery,
    sendMessage,
    cancelRequest,
    clearMessages,
    setCurrentQuery
  };
};
