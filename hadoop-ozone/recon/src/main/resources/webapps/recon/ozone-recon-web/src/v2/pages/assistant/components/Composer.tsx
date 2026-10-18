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

import React, { useState, KeyboardEvent } from 'react';
import { Input, Button } from 'antd';
import { SendOutlined, StopOutlined } from '@ant-design/icons';
import ModelPicker from './ModelPicker';

interface ComposerProps {
  onSend: (query: string, model?: string, provider?: string) => void;
  onCancel: () => void;
  isInFlight: boolean;
  models: string[];
  currentQuery: string;
  setCurrentQuery: (q: string) => void;
}

const Composer: React.FC<ComposerProps> = ({ 
  onSend, 
  onCancel, 
  isInFlight, 
  models,
  currentQuery,
  setCurrentQuery
}) => {
  const [selectedProvider, setSelectedProvider] = useState<string | undefined>();
  const [selectedModel, setSelectedModel] = useState<string | undefined>();

  const handleProviderChange = (provider: string) => {
    setSelectedProvider(provider);
    // Reset model when provider changes to avoid invalid combinations
    setSelectedModel(undefined);
  };

  const handleSend = () => {
    if (currentQuery.trim() && !isInFlight) {
      onSend(currentQuery, selectedModel, selectedProvider);
    }
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="composer-container">
      <div className="composer-pill">
        <div className="composer-toolbar">
          <ModelPicker 
            models={models} 
            selectedProvider={selectedProvider}
            selectedModel={selectedModel} 
            onProviderChange={handleProviderChange}
            onModelChange={setSelectedModel} 
            disabled={isInFlight} 
          />
        </div>
        <div className="composer-input-wrapper">
          <Input.TextArea
            value={currentQuery}
            onChange={(e) => setCurrentQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask Recon AI about your cluster..."
            autoSize={{ minRows: 1, maxRows: 6 }}
            disabled={isInFlight}
            className="composer-textarea"
            bordered={false}
          />
          <div className="composer-actions">
            {isInFlight ? (
              <Button 
                type="primary" 
                danger 
                icon={<StopOutlined />} 
                onClick={onCancel}
                shape="circle"
                className="stop-btn"
              />
            ) : (
              <Button 
                type="primary" 
                icon={<SendOutlined />} 
                onClick={handleSend}
                disabled={!currentQuery.trim()}
                shape="circle"
                className="send-btn"
              />
            )}
          </div>
        </div>
      </div>
      <div className="composer-disclaimer">
        Recon AI treats each query independently and may occasionally hallucinate. Please verify important cluster data.
      </div>
    </div>
  );
};

export default Composer;
