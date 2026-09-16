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

export const CHATBOT_ENDPOINTS = {
  HEALTH: '/api/v1/chatbot/health',
  MODELS: '/api/v1/chatbot/models',
  CHAT: '/api/v1/chatbot/chat'
};

export const DEFAULT_MODEL_SENTINEL = 'default_server_model';

export const PROVIDER_LABELS: Record<string, string> = {
  openai: 'OpenAI',
  gemini: 'Google Gemini',
  anthropic: 'Anthropic Claude',
  other: 'Other Models'
};

export const getProviderForModel = (model: string): string => {
  const lower = model.toLowerCase();
  if (lower.startsWith('gpt') || lower.startsWith('o1') || lower.startsWith('o3')) return 'openai';
  if (lower.startsWith('gemini')) return 'gemini';
  if (lower.startsWith('claude')) return 'anthropic';
  return 'other';
};

export const RECON_LOGS_HINT = 'For more details, check the Recon server logs.';

const GENERIC_BACKEND_PROCESSING_ERROR = 'An error occurred processing your request.';

export const resolveRequestProvider = (provider?: string, model?: string): string | undefined => {
  if (provider && provider !== DEFAULT_MODEL_SENTINEL) {
    return provider;
  }
  if (model && model !== DEFAULT_MODEL_SENTINEL) {
    return getProviderForModel(model);
  }
  return undefined;
};

export const isMaskedLlmProcessingError = (errorText?: string): boolean => {
  if (!errorText) {
    return true;
  }
  if (errorText === GENERIC_BACKEND_PROCESSING_ERROR) {
    return true;
  }
  return (
    errorText.includes('OpenAiHttpException')
    || errorText.includes('LLM request failed')
    || errorText.includes('An internal error occurred while processing your request')
  );
};

export const getLlmProviderFailureMessage = (provider?: string, model?: string): string => {
  const resolved = resolveRequestProvider(provider, model);

  let main: string;
  switch (resolved) {
    case 'openai':
      main = 'OpenAI could not complete this request. Check your API key and model settings in ozone-site.xml, or try another provider such as Google Gemini.';
      break;
    case 'gemini':
      main = 'Google Gemini could not complete this request. Check your API key and model settings in ozone-site.xml, or try another provider such as OpenAI.';
      break;
    case 'anthropic':
      main = 'Anthropic Claude could not complete this request. Check your API key and model settings in ozone-site.xml, or try another provider such as OpenAI.';
      break;
    default:
      main = 'An error occurred while processing your request. Please try again or switch to a different model or provider.';
      break;
  }

  return `${main}\n\n${RECON_LOGS_HINT}`;
};

export const LOADING_STAGES = [
  { maxSeconds: 3, text: "Understanding query..." },
  { maxSeconds: 10, text: "Searching cluster data..." },
  { maxSeconds: 45, text: "Summarizing results..." },
  { maxSeconds: Infinity, text: "Still working - complex queries can take a moment..." }
];

export const SEED_PROMPTS = [
  {
    icon: 'heart',
    label: 'Cluster Health',
    prompt: "How many unhealthy containers are there?"
  },
  {
    icon: 'database',
    label: 'Volumes & Buckets',
    prompt: 'Give me a high-level summary of volumes and buckets in the cluster'
  },
  {
    icon: 'pie-chart',
    label: 'Capacity',
    prompt: "What is the current cluster capacity?"
  },
  {
    icon: 'schedule',
    label: 'OM Tasks',
    prompt: "Show me the status of OM tasks"
  },
  {
    icon: 'cluster',
    label: 'Datanodes',
    prompt: "List the datanodes in the cluster"
  },
  {
    icon: 'deployment-unit',
    label: 'Pipelines',
    prompt: "Show me the pipelines"
  }
];
