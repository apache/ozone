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

import React, { useEffect, useRef } from 'react';
import MessageBubble from './MessageBubble';
import { ChatMessage } from '@/v2/types/chatbot.types';
import LoadingIndicator from './LoadingIndicator';
import { Button } from 'antd';
import ReconAIMark from './ReconAIMark';

interface MessageListProps {
  messages: ChatMessage[];
  isInFlight: boolean;
  elapsedSeconds: number;
  errorBubble: string | null;
  onRetry: () => void;
  onRegenerate: (index: number) => void;
}

const MessageList: React.FC<MessageListProps> = ({ 
  messages, 
  isInFlight, 
  elapsedSeconds, 
  errorBubble,
  onRetry,
  onRegenerate
}) => {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, isInFlight, errorBubble]);

  return (
    <div className="message-list">
      {messages.map((msg, index) => (
        <MessageBubble 
          key={msg.id} 
          message={msg} 
          onRegenerate={msg.role === 'assistant' ? () => onRegenerate(index) : undefined} 
        />
      ))}
      
      {isInFlight && (
        <LoadingIndicator elapsedSeconds={elapsedSeconds} />
      )}
      
      {errorBubble && (
        <div className="message-bubble-wrapper assistant-message error-bubble">
          <div className="message-bubble-content">
            <div className="message-avatar">
              <ReconAIMark size={20} />
            </div>
            <div className="message-bubble-body">
              <div className="error-content">
                <span className="error-icon">⚠️</span>
                <span className="error-text">{errorBubble}</span>
              </div>
              <div className="message-actions" style={{ marginLeft: 0, marginTop: '12px', opacity: 1 }}>
                <Button size="small" onClick={onRetry} type="primary" ghost>
                  Retry
                </Button>
              </div>
            </div>
          </div>
        </div>
      )}
      
      <div ref={bottomRef} />
    </div>
  );
};

export default MessageList;
