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

import React, { useState, useRef, useEffect } from 'react';
import {
  Input, Button, Card, Row, Col, Typography, Space,
  Avatar, Tag, message as antdMessage, Tooltip, Select
} from 'antd';
import {
  SendOutlined, UserOutlined, ThunderboltOutlined,
  CopyOutlined, DeleteOutlined
} from '@ant-design/icons';
import './AIAssistant.less';

const { Title, Paragraph } = Typography;
const { Option } = Select;

interface Message {
  text: string;
  sender: 'user' | 'bot';
  timestamp: Date;
}

// Provider → model mapping (1M-context models only).
const PROVIDER_MODELS: Record<string, string[]> = {
  gemini: ['gemini-2.5-flash', 'gemini-2.5-pro', 'gemini-3-flash-preview', 'gemini-3.1-pro-preview'],
  openai: ['gpt-4.1', 'gpt-4.1-mini', 'gpt-4.1-nano'],
  anthropic: ['claude-sonnet-4-6', 'claude-opus-4-6']
};

const PROVIDER_LABELS: Record<string, string> = {
  gemini: 'Google Gemini',
  openai: 'OpenAI',
  anthropic: 'Anthropic Claude'
};

const SUGGESTED_QUESTIONS = [
  'How many datanodes are in the cluster?',
  'What is the current state of the cluster?',
  'Are there any unhealthy containers?',
  'What is the storage usage of each datanode?',
  'How many open keys are there?',
  'Show me the pipeline status'
];

const CHAT_STORAGE_KEY = 'ozone-recon-chat-history';

const AIAssistant: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [selectedProvider, setSelectedProvider] = useState('gemini');
  const [selectedModel, setSelectedModel] = useState('gemini-2.5-flash');
  const chatWindowRef = useRef<HTMLDivElement>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Load chat history on mount
  useEffect(() => {
    try {
      const saved = localStorage.getItem(CHAT_STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved);
        setMessages(parsed.map((msg: any) => ({
          ...msg,
          timestamp: new Date(msg.timestamp)
        })));
      }
    } catch (err) {
      console.error('Failed to load chat history:', err);
    }
  }, []);

  // Persist chat history
  useEffect(() => {
    if (messages.length > 0) {
      try {
        localStorage.setItem(CHAT_STORAGE_KEY, JSON.stringify(messages));
      } catch (err) {
        console.error('Failed to save chat history:', err);
      }
    }
  }, [messages]);

  // Auto-scroll
  useEffect(() => {
    if (chatWindowRef.current) {
      chatWindowRef.current.scrollTop = chatWindowRef.current.scrollHeight;
    }
  }, [messages, isLoading]);

  const handleProviderChange = (provider: string) => {
    setSelectedProvider(provider);
    const models = PROVIDER_MODELS[provider] || [];
    setSelectedModel(models[0] || '');
  };

  const sendMessage = async (query: string) => {
    if (query.trim() === '') return;

    const userMessage: Message = {
      text: query,
      sender: 'user',
      timestamp: new Date()
    };
    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);

    try {
      const response = await fetch('/api/v1/chatbot/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query,
          model: selectedModel,
          provider: selectedProvider
        })
      });

      if (!response.ok) {
        const errData = await response.json().catch(() => ({}));
        throw new Error(errData.error || 'Network response was not ok');
      }

      const data = await response.json();
      const botMessage: Message = {
        text: data.response || data.error || 'No response received.',
        sender: 'bot',
        timestamp: new Date()
      };
      setMessages(prev => [...prev, botMessage]);
    } catch (error: any) {
      setMessages(prev => [...prev, {
        text: `Error: ${error.message || 'Something went wrong.'}`,
        sender: 'bot',
        timestamp: new Date()
      }]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleCopyMessage = (text: string) => {
    const div = document.createElement('div');
    div.innerHTML = text;
    const plain = div.textContent || div.innerText || '';
    navigator.clipboard.writeText(plain).then(
        () => antdMessage.success('Copied to clipboard'),
        () => antdMessage.error('Failed to copy')
    );
  };

  const handleClearChat = () => {
    setMessages([]);
    localStorage.removeItem(CHAT_STORAGE_KEY);
    antdMessage.success('Chat history cleared');
  };

  const formatTimestamp = (date: Date) => {
    const diffMins = Math.floor((Date.now() - date.getTime()) / 60000);
    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    return date.toLocaleTimeString('en-US', {
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  return (
      <div className="ai-assistant-container">
        <div className="ai-assistant-header">
          <Space style={{ width: '100%', justifyContent: 'space-between' }}>
            <Space>
              <Avatar
                  src="/ai-assistant-logo.png"
                  size="large"
                  className="assistant-avatar"
                  style={{ background: 'transparent' }}
              />
              <div>
                <Title level={3} style={{ margin: 0 }}>AI Assistant</Title>
                <Paragraph type="secondary" style={{ margin: 0, fontSize: '12px' }}>
                  Ask questions about your Ozone cluster
                </Paragraph>
              </div>
            </Space>
            {messages.length > 0 && (
                <Tooltip title="Clear conversation">
                  <Button
                      icon={<DeleteOutlined />}
                      onClick={handleClearChat}
                      size="small"
                      danger
                      type="text"
                  >
                    Clear Chat
                  </Button>
                </Tooltip>
            )}
          </Space>
        </div>

        <Card className="chat-card" bordered={false}>
          <div className="chat-window" ref={chatWindowRef}>
            {messages.length === 0 && (
                <div className="empty-state">
                  <img
                      src="/ai-assistant-logo.png"
                      alt="AI Assistant"
                      className="empty-icon"
                      style={{
                        width: '120px',
                        height: '120px',
                        objectFit: 'contain'
                      }}
                  />
                  <Title level={4}>Welcome to Ozone AI Assistant</Title>
                  <Paragraph type="secondary" style={{ marginBottom: '24px' }}>
                    I can help you understand your cluster's health, datanodes,
                    containers, and more.
                  </Paragraph>
                  <div className="suggestions-container">
                    <Space direction="vertical" size="small"
                           style={{ width: '100%' }}>
                      <Title level={5}
                             style={{ marginBottom: '12px', color: '#667eea' }}>
                        <ThunderboltOutlined /> Quick Questions
                      </Title>
                      <Space size={[8, 8]} wrap>
                        {SUGGESTED_QUESTIONS.map((q, i) => (
                            <Tag
                                key={i}
                                className="suggestion-tag"
                                onClick={() => sendMessage(q)}
                            >
                              {q}
                            </Tag>
                        ))}
                      </Space>
                    </Space>
                  </div>
                </div>
            )}

            {messages.map((message, index) => (
                <div key={index}
                     className={`message-wrapper ${message.sender}`}>
                  <div className="message-content">
                    {message.sender === 'bot' && (
                        <Avatar
                            src="/ai-assistant-logo.png"
                            className="message-avatar bot-avatar"
                            size="small"
                            style={{ background: 'white' }}
                        />
                    )}
                    <div className={`message-bubble ${message.sender}`}>
                      <div className="message-text">
                        {message.text.split('\n').map((line, i, arr) => (
                            <React.Fragment key={i}>
                              {line}
                              {i < arr.length - 1 && <br />}
                            </React.Fragment>
                        ))}
                      </div>
                      <div className="message-footer">
                                        <span className="message-timestamp">
                                            {formatTimestamp(message.timestamp)}
                                        </span>
                        {message.sender === 'bot' && (
                            <Tooltip title="Copy response">
                              <Button
                                  type="text"
                                  size="small"
                                  icon={<CopyOutlined />}
                                  onClick={() => handleCopyMessage(message.text)}
                                  className="copy-button"
                              />
                            </Tooltip>
                        )}
                      </div>
                    </div>
                    {message.sender === 'user' && (
                        <Avatar
                            icon={<UserOutlined />}
                            className="message-avatar user-avatar"
                            size="small"
                        />
                    )}
                  </div>
                </div>
            ))}

            {isLoading && (
                <div className="message-wrapper bot">
                  <div className="message-content">
                    <Avatar
                        src="/ai-assistant-logo.png"
                        className="message-avatar bot-avatar"
                        size="small"
                        style={{ background: 'white' }}
                    />
                    <div className="typing-indicator">
                      <span></span>
                      <span></span>
                      <span></span>
                    </div>
                  </div>
                </div>
            )}
            <div ref={messagesEndRef} />
          </div>

          {/* Provider / Model selector + Input */}
          <div className="input-container">
            <div className="model-selector-bar">
              <Select
                  id="provider-select"
                  value={selectedProvider}
                  onChange={handleProviderChange}
                  style={{ width: 170 }}
                  size="small"
              >
                {Object.entries(PROVIDER_LABELS).map(([key, label]) => (
                    <Option key={key} value={key}>{label}</Option>
                ))}
              </Select>
              <Select
                  id="model-select"
                  value={selectedModel}
                  onChange={(val: string) => setSelectedModel(val)}
                  style={{ width: 200 }}
                  size="small"
              >
                {(PROVIDER_MODELS[selectedProvider] || []).map(m => (
                    <Option key={m} value={m}>{m}</Option>
                ))}
              </Select>
            </div>
            <Row gutter={12} align="middle">
              <Col flex="auto">
                <Input
                    id="chat-input"
                    value={inputValue}
                    onChange={e => setInputValue(e.target.value)}
                    onPressEnter={() => sendMessage(inputValue)}
                    placeholder="Ask a question about your Ozone cluster..."
                    disabled={isLoading}
                    size="large"
                    className="chat-input"
                />
              </Col>
              <Col>
                <Button
                    id="send-button"
                    type="primary"
                    icon={<SendOutlined />}
                    onClick={() => sendMessage(inputValue)}
                    loading={isLoading}
                    size="large"
                    className="send-button"
                >
                  Send
                </Button>
              </Col>
            </Row>
          </div>
        </Card>
      </div>
  );
};

export default AIAssistant;
