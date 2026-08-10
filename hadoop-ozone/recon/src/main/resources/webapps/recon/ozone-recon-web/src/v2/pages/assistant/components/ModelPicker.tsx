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

import React, { useMemo } from 'react';
import { Select } from 'antd';
import { DEFAULT_MODEL_SENTINEL, getProviderForModel, PROVIDER_LABELS } from '@/v2/constants/chatbot.constants';

const { Option, OptGroup } = Select;

interface ModelPickerProps {
  models: string[];
  selectedProvider?: string;
  selectedModel?: string;
  onProviderChange: (provider: string) => void;
  onModelChange: (model: string) => void;
  disabled?: boolean;
}

const ModelPicker: React.FC<ModelPickerProps> = ({ 
  models, 
  selectedProvider,
  selectedModel, 
  onProviderChange,
  onModelChange, 
  disabled 
}) => {
  const groupedModels = useMemo(() => {
    const groups: Record<string, string[]> = {
      openai: [],
      gemini: [],
      anthropic: [],
      other: []
    };
    
    models.forEach(model => {
      const provider = getProviderForModel(model);
      if (groups[provider]) {
        groups[provider].push(model);
      }
    });
    
    return groups;
  }, [models]);

  const availableProviders = useMemo(() => {
    return Object.keys(groupedModels).filter(key => groupedModels[key].length > 0);
  }, [groupedModels]);

  const availableModelsForProvider = useMemo(() => {
    if (!selectedProvider || selectedProvider === DEFAULT_MODEL_SENTINEL) {
      return [];
    }
    return groupedModels[selectedProvider] || [];
  }, [selectedProvider, groupedModels]);

  return (
    <div style={{ display: 'flex', gap: '8px' }}>
      <Select
        value={selectedProvider || DEFAULT_MODEL_SENTINEL}
        onChange={onProviderChange}
        disabled={disabled}
        className="model-picker"
        dropdownClassName="ai-picker-dropdown"
        size="small"
        dropdownMatchSelectWidth={false}
      >
        <Option value={DEFAULT_MODEL_SENTINEL}>Default Provider</Option>
        {availableProviders.map(providerKey => (
          <Option key={providerKey} value={providerKey}>
            {PROVIDER_LABELS[providerKey] || providerKey}
          </Option>
        ))}
      </Select>

      {selectedProvider && selectedProvider !== DEFAULT_MODEL_SENTINEL && (
        <Select
          value={selectedModel || DEFAULT_MODEL_SENTINEL}
          onChange={onModelChange}
          disabled={disabled || availableModelsForProvider.length === 0}
          className="model-picker"
          dropdownClassName="ai-picker-dropdown"
          size="small"
          dropdownMatchSelectWidth={false}
        >
          <Option value={DEFAULT_MODEL_SENTINEL}>Default Model</Option>
          {availableModelsForProvider.map(model => (
            <Option key={model} value={model}>{model}</Option>
          ))}
        </Select>
      )}
    </div>
  );
};

export default ModelPicker;
