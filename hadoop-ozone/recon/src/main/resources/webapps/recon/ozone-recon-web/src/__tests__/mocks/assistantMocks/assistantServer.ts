/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { delay, http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { CHATBOT_ENDPOINTS } from '@/v2/constants/chatbot.constants';

export const mockHealthEnabled = http.get(CHATBOT_ENDPOINTS.HEALTH, () => {
  return HttpResponse.json({
    enabled: true,
    llmClientAvailable: true,
  });
});

export const mockHealthDisabled = http.get(CHATBOT_ENDPOINTS.HEALTH, () => {
  return HttpResponse.json({
    enabled: false,
    llmClientAvailable: true,
  });
});

export const mockHealthNotConfigured = http.get(CHATBOT_ENDPOINTS.HEALTH, () => {
  return HttpResponse.json({
    enabled: true,
    llmClientAvailable: false,
  });
});

export const mockModels = http.get(CHATBOT_ENDPOINTS.MODELS, () => {
  return HttpResponse.json({
    models: ['gpt-4.1-nano', 'gemini-2.5-flash', 'gemini-2.5-pro', 'claude-opus-4-6'],
  });
});

export const mockModelsError = http.get(CHATBOT_ENDPOINTS.MODELS, () => {
  return HttpResponse.json({
    error: 'Failed to fetch models',
  }, { status: 500 });
});

export const mockModelsDisabled = http.get(CHATBOT_ENDPOINTS.MODELS, () => {
  return HttpResponse.json({
    error: 'Chatbot service is not enabled',
  }, { status: 503 });
});

export const mockChatSuccess = http.post(CHATBOT_ENDPOINTS.CHAT, () => {
  return HttpResponse.json({
    response: 'This is a **Markdown** response with a table:\n\n| Col 1 | Col 2 |\n|---|---|\n| A | B |',
    success: true,
  });
});

export const mockChatDelayed = http.post(CHATBOT_ENDPOINTS.CHAT, async () => {
  await delay(100);
  return HttpResponse.json({
    response: 'Delayed',
    success: true,
  }, { status: 200 });
});

export const mockChatBusy = http.post(CHATBOT_ENDPOINTS.CHAT, () => {
  return HttpResponse.json({
    error: 'The chatbot is currently handling too many requests. Please try again in a moment.'
  }, { status: 503 });
});

export const mockChatTimeout = http.post(CHATBOT_ENDPOINTS.CHAT, () => {
  return HttpResponse.json({
    error: 'The chatbot request timed out. The LLM or Recon API took too long to respond. Please try again or use a different model.',
  }, { status: 504 });
});

export const mockChatError = http.post(CHATBOT_ENDPOINTS.CHAT, () => {
  return HttpResponse.json({
    error: 'An error occurred processing your request.',
  }, { status: 500 });
});

export const mockChatDisabled = http.post(CHATBOT_ENDPOINTS.CHAT, () => {
  return HttpResponse.json({
    error: 'Chatbot service is not enabled',
  }, { status: 503 });
});

export const mockChatInterrupted = http.post(CHATBOT_ENDPOINTS.CHAT, () => {
  return HttpResponse.json({
    error: 'Request was interrupted. Please try again.',
  }, { status: 503 });
});

export const mockChatEmpty = http.post(CHATBOT_ENDPOINTS.CHAT, () => {
  return HttpResponse.json({
    error: 'Query cannot be empty',
  }, { status: 400 });
});

export const assistantServer = setupServer(
  mockHealthEnabled,
  mockModels,
  mockChatSuccess
);
