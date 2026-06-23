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
import { SEED_PROMPTS } from '@/v2/constants/chatbot.constants';
import { 
  HeartOutlined, 
  PieChartOutlined, 
  DatabaseOutlined, 
  ScheduleOutlined,
  ClusterOutlined, 
  DeploymentUnitOutlined,
  ArrowRightOutlined
} from '@ant-design/icons';
import ReconAIMark from './ReconAIMark';

interface EmptyStateProps {
  onPromptClick: (prompt: string) => void;
}

const getIcon = (iconName: string) => {
  switch (iconName) {
    case 'heart': return <HeartOutlined />;
    case 'database': return <DatabaseOutlined />;
    case 'pie-chart': return <PieChartOutlined />;
    case 'schedule': return <ScheduleOutlined />;
    case 'cluster': return <ClusterOutlined />;
    case 'deployment-unit': return <DeploymentUnitOutlined />;
    default: return <ReconAIMark size={20} />;
  }
};

const EmptyState: React.FC<EmptyStateProps> = ({ onPromptClick }) => {
  return (
    <div className="empty-state-container">
      <div className="empty-state-hero">
        <div className="hero-orb">
          <ReconAIMark size={40} />
        </div>
        <h2>Welcome to Recon AI</h2>
        <p>Ask questions about your Ozone cluster's health, capacity, and metadata.</p>
      </div>
      
      <div className="seed-prompts-grid">
        {SEED_PROMPTS.map((item, index) => (
          <div 
            key={index} 
            className="seed-prompt-chip" 
            onClick={() => onPromptClick(item.prompt)}
          >
            <div className="chip-icon">
              {getIcon(item.icon)}
            </div>
            <div className="chip-content">
              <span className="chip-label">{item.label}</span>
              <span className="chip-prompt">{item.prompt}</span>
            </div>
            <div className="chip-arrow">
              <ArrowRightOutlined />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default EmptyState;
