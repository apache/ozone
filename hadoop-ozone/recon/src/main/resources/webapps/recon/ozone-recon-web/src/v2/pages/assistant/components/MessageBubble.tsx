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

import React from 'react';
import { Button, Tooltip } from 'antd';
import { CopyOutlined, CheckOutlined, UserOutlined, ReloadOutlined } from '@ant-design/icons';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { ChatMessage } from '@/v2/types/chatbot.types';
import classNames from 'classnames';
import { copyToClipboard } from '@/utils/clipboard';
import ReconAIMark from './ReconAIMark';

interface MessageBubbleProps {
  message: ChatMessage;
  onRegenerate?: () => void;
}

const MessageBubble: React.FC<MessageBubbleProps> = ({ message, onRegenerate }) => {
  const [copied, setCopied] = React.useState(false);
  const [copyFailed, setCopyFailed] = React.useState(false);

  const handleCopy = async () => {
    const didCopy = await copyToClipboard(message.text);
    if (didCopy) {
      setCopyFailed(false);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } else {
      setCopied(false);
      setCopyFailed(true);
      setTimeout(() => setCopyFailed(false), 2000);
    }
  };

  const isUser = message.role === 'user';

  return (
    <div className={classNames('message-bubble-wrapper', { 'user-message': isUser, 'assistant-message': !isUser })}>
      <div className="message-bubble-content">
        <div className="message-avatar">
          {isUser ? <UserOutlined /> : <ReconAIMark size={20} />}
        </div>
        <div className="message-bubble-body">
          {isUser ? (
            <div className="message-text">{message.text}</div>
          ) : (
            <div className="message-markdown">
              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                {message.text}
              </ReactMarkdown>
            </div>
          )}
        </div>
      </div>
      {!isUser && (
        <div className="message-actions">
          <Tooltip title={copied ? 'Copied!' : copyFailed ? 'Copy failed' : 'Copy'}>
            <Button 
              type="text" 
              size="small" 
              icon={copied ? <CheckOutlined /> : <CopyOutlined />} 
              onClick={handleCopy}
            />
          </Tooltip>
          {onRegenerate && (
            <Tooltip title="Regenerate">
              <Button 
                type="text" 
                size="small" 
                icon={<ReloadOutlined />} 
                onClick={onRegenerate}
              />
            </Tooltip>
          )}
        </div>
      )}
    </div>
  );
};

export default MessageBubble;
