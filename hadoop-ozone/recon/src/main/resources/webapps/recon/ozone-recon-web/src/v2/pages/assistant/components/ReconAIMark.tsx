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

interface ReconAIMarkProps {
  size?: number;
  active?: boolean;
  className?: string;
}

const ReconAIMark: React.FC<ReconAIMarkProps> = ({ size = 24, active = false, className = '' }) => {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="currentColor"
      xmlns="http://www.w3.org/2000/svg"
      className={`recon-ai-mark ${active ? 'active' : ''} ${className}`}
      style={{ display: 'inline-block', verticalAlign: 'middle' }}
    >
      <defs>
        <path id="sparkle" d="M 0 -10 C 0 -4 4 0 10 0 C 4 0 0 4 0 10 C 0 4 -4 0 -10 0 C -4 0 0 -4 0 -10 Z" />
      </defs>
      <g className="spark-group" style={{ transformOrigin: '12px 12px' }}>
        {/* Main large star */}
        <use href="#sparkle" transform="translate(8, 12) scale(0.8)" />
        {/* Medium star right */}
        <use href="#sparkle" transform="translate(19, 12) scale(0.4)" />
        {/* Small star top right */}
        <use href="#sparkle" transform="translate(14, 5) scale(0.25)" />
        {/* Small star bottom right */}
        <use href="#sparkle" transform="translate(14, 19) scale(0.25)" />
      </g>
    </svg>
  );
};

export default ReconAIMark;
