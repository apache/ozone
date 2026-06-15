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

import { rest } from 'msw';
import { setupServer } from 'msw/node';
import { CHATBOT_ENDPOINTS } from '@/v2/constants/chatbot.constants';

export const mockHealthEnabled = rest.get(CHATBOT_ENDPOINTS.HEALTH, (req, res, ctx) => {
  return res(
    ctx.status(200),
    ctx.json({
      enabled: true,
      llmClientAvailable: true
    })
  );
});

export const mockHealthDisabled = rest.get(CHATBOT_ENDPOINTS.HEALTH, (req, res, ctx) => {
  return res(
    ctx.status(200),
    ctx.json({
      enabled: false,
      llmClientAvailable: true
    })
  );
});

export const mockHealthNotConfigured = rest.get(CHATBOT_ENDPOINTS.HEALTH, (req, res, ctx) => {
  return res(
    ctx.status(200),
    ctx.json({
      enabled: true,
      llmClientAvailable: false
    })
  );
});

export const mockModels = rest.get(CHATBOT_ENDPOINTS.MODELS, (req, res, ctx) => {
  return res(
    ctx.status(200),
    ctx.json({
      models: ['gpt-4.1-nano', 'gemini-2.5-flash', 'gemini-2.5-pro', 'claude-opus-4-6']
    })
  );
});

export const mockModelsError = rest.get(CHATBOT_ENDPOINTS.MODELS, (req, res, ctx) => {
  return res(
    ctx.status(500),
    ctx.json({
      error: 'Failed to fetch models'
    })
  );
});

export const mockModelsDisabled = rest.get(CHATBOT_ENDPOINTS.MODELS, (req, res, ctx) => {
  return res(
    ctx.status(503),
    ctx.json({
      error: 'Chatbot service is not enabled'
    })
  );
});

export const mockChatSuccess = rest.post(CHATBOT_ENDPOINTS.CHAT, (req, res, ctx) => {
  return res(
    ctx.status(200),
    ctx.json({
      response: 'This is a **Markdown** response with a table:\n\n| Col 1 | Col 2 |\n|---|---|\n| A | B |',
      success: true
    })
  );
});

export const mockChatDelayed = rest.post(CHATBOT_ENDPOINTS.CHAT, (req, res, ctx) => {
  return res(
    ctx.delay(100),
    ctx.status(200),
    ctx.json({
      response: 'Delayed',
      success: true
    })
  );
});

export const mockChatBusy = rest.post(CHATBOT_ENDPOINTS.CHAT, (req, res, ctx) => {
  return res(
    ctx.status(503),
    ctx.json({
      error: 'The chatbot is currently handling too many requests. Please try again in a moment.'
    })
  );
});

export const mockChatTimeout = rest.post(CHATBOT_ENDPOINTS.CHAT, (req, res, ctx) => {
  return res(
    ctx.status(504),
    ctx.json({
      error: 'The chatbot request timed out. The LLM or Recon API took too long to respond. Please try again or use a different model.'
    })
  );
});

export const mockChatError = rest.post(CHATBOT_ENDPOINTS.CHAT, (req, res, ctx) => {
  return res(
    ctx.status(500),
    ctx.json({
      error: 'An error occurred processing your request.'
    })
  );
});

export const mockChatDisabled = rest.post(CHATBOT_ENDPOINTS.CHAT, (req, res, ctx) => {
  return res(
    ctx.status(503),
    ctx.json({
      error: 'Chatbot service is not enabled'
    })
  );
});

export const mockChatInterrupted = rest.post(CHATBOT_ENDPOINTS.CHAT, (req, res, ctx) => {
  return res(
    ctx.status(503),
    ctx.json({
      error: 'Request was interrupted. Please try again.'
    })
  );
});

export const mockChatEmpty = rest.post(CHATBOT_ENDPOINTS.CHAT, (req, res, ctx) => {
  return res(
    ctx.status(400),
    ctx.json({
      error: 'Query cannot be empty'
    })
  );
});

export const assistantServer = setupServer(
  mockHealthEnabled,
  mockModels,
  mockChatSuccess
);
