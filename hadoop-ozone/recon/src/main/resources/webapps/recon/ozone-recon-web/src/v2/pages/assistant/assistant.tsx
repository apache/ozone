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

import React, { useEffect, useState } from 'react';
import { useApiData } from '@/v2/hooks/useAPIData.hook';
import { showDataFetchError } from '@/utils/common';
import { ChatbotHealthResponse, ChatbotModelsResponse } from '@/v2/types/chatbot.types';
import { CHATBOT_ENDPOINTS } from '@/v2/constants/chatbot.constants';
import { useChat } from '@/v2/hooks/useChat.hook';
import { Spin, Button, Tag } from 'antd';
import { ClearOutlined } from '@ant-design/icons';

import DisabledState from './components/DisabledState';
import EmptyState from './components/EmptyState';
import MessageList from './components/MessageList';
import Composer from './components/Composer';

import './assistant.less';

import ReconAIMark from './components/ReconAIMark';

const Assistant: React.FC = () => {
  const [isHealthLoaded, setIsHealthLoaded] = useState(false);
  const [isModelsLoaded, setIsModelsLoaded] = useState(false);

  const healthData = useApiData<ChatbotHealthResponse>(
    CHATBOT_ENDPOINTS.HEALTH,
    { enabled: false, llmClientAvailable: false },
    {
      onSuccess: () => setIsHealthLoaded(true),
      onError: (error) => {
        showDataFetchError(error);
        setIsHealthLoaded(true); // Still set loaded to show disabled state
      }
    }
  );

  const modelsData = useApiData<ChatbotModelsResponse>(
    CHATBOT_ENDPOINTS.MODELS,
    { models: [] },
    {
      initialFetch: false,
      retryAttempts: 0,
      onSuccess: () => setIsModelsLoaded(true),
      onError: (error) => {
        showDataFetchError(error);
        setIsModelsLoaded(true);
      }
    }
  );

  const {
    messages,
    isInFlight,
    elapsedSeconds,
    errorBubble,
    currentQuery,
    setCurrentQuery,
    sendMessage,
    cancelRequest,
    clearMessages
  } = useChat();

  useEffect(() => {
    if (isHealthLoaded && healthData.data.enabled && healthData.data.llmClientAvailable) {
      modelsData.execute().catch(() => {});
    }
  }, [isHealthLoaded, healthData.data]);

  if (!isHealthLoaded || (healthData.data.enabled && healthData.data.llmClientAvailable && !isModelsLoaded)) {
    return (
      <div className="assistant-page-container" style={{ alignItems: 'center', justifyContent: 'center' }}>
        <Spin size="large" />
      </div>
    );
  }

  if (!healthData.data.enabled) {
    return (
      <div className="assistant-page-container">
        <DisabledState reason="disabled" />
      </div>
    );
  }

  if (!healthData.data.llmClientAvailable) {
    return (
      <div className="assistant-page-container">
        <DisabledState reason="not-configured" />
      </div>
    );
  }

  const handlePromptClick = (prompt: string) => {
    setCurrentQuery(prompt);
  };

  const handleRetry = () => {
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i].role === 'user') {
        sendMessage(messages[i].text, messages[i].model, messages[i].provider);
        break;
      }
    }
  };

  const handleRegenerate = (index: number) => {
    // Find the last user message before this assistant message
    for (let i = index - 1; i >= 0; i--) {
      if (messages[i].role === 'user') {
        sendMessage(messages[i].text, messages[i].model, messages[i].provider);
        break;
      }
    }
  };

  return (
    <div className="assistant-page-container">
      <div className="assistant-header">
        <div className="header-left">
          <div className="header-icon-wrapper">
            <ReconAIMark size={24} active={isInFlight} />
          </div>
          <div className="header-text">
            <div className="header-title-row">
              <h1 className="header-title">Recon AI</h1>
              <Tag className="alpha-tag">Alpha</Tag>
            </div>
            <div className="header-subtitle">Your intelligent cluster assistant</div>
          </div>
        </div>
        {messages.length > 0 && (
          <Button 
            icon={<ClearOutlined />} 
            onClick={clearMessages}
            type="default"
            className="new-chat-btn"
            disabled={isInFlight}
          >
            New chat
          </Button>
        )}
      </div>
      <div className="assistant-chat-area">
        {messages.length === 0 ? (
          <EmptyState onPromptClick={handlePromptClick} />
        ) : (
          <MessageList 
            messages={messages}
            isInFlight={isInFlight}
            elapsedSeconds={elapsedSeconds}
            errorBubble={errorBubble}
            onRetry={handleRetry}
            onRegenerate={handleRegenerate}
          />
        )}
      </div>
      <Composer 
        onSend={sendMessage}
        onCancel={cancelRequest}
        isInFlight={isInFlight}
        models={modelsData.data.models}
        currentQuery={currentQuery}
        setCurrentQuery={setCurrentQuery}
      />
    </div>
  );
};

export default Assistant;
