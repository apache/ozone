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
import ReconAIMark from './ReconAIMark';
import { LOADING_STAGES } from '@/v2/constants/chatbot.constants';

interface LoadingIndicatorProps {
  elapsedSeconds: number;
}

const LoadingIndicator: React.FC<LoadingIndicatorProps> = ({ elapsedSeconds }) => {
  const isLong = elapsedSeconds > 60;

  const currentStageText = useMemo(() => {
    for (const stage of LOADING_STAGES) {
      if (elapsedSeconds <= stage.maxSeconds) {
        return stage.text;
      }
    }
    return LOADING_STAGES[LOADING_STAGES.length - 1].text;
  }, [elapsedSeconds]);

  return (
    <div className="message-bubble-wrapper assistant-message loading-bubble">
      <div className="message-bubble-content">
        <div className="message-avatar">
          <ReconAIMark size={20} active={true} />
        </div>
        <div className="message-bubble-body">
          <div className="skeleton-line"></div>
          <div className="skeleton-line"></div>
          <div className="skeleton-line short"></div>
          
          <div className="loading-status-row" role="status" aria-live="polite">
            <span className="loading-text">{currentStageText}</span>
            <div className="typing-dots">
              <div className="dot"></div>
              <div className="dot"></div>
              <div className="dot"></div>
            </div>
          </div>
          
          {isLong && (
            <div className="loading-warning">
              This is taking longer than usual. The request may time out after 3 minutes.
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default LoadingIndicator;
