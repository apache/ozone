/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software

 * distributed under the License is distributed on an "AS IS" BASIS,

 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React, { useMemo } from 'react';

type StackedProgressProps = {
  segments: Segment[];
};

const StackedProgress: React.FC<StackedProgressProps> = ({
  segments,
}) => {
  const total = useMemo(() => {
    return segments.reduce((sum, item) => sum + item.value, 0);
  }, [segments]);

  // Handle the case where there is no data to show
  if (!total || total === 0) {
    return (
      <div className='stacked-progress-empty' />
    );
  }

  return (
    <div className='stacked-progress'>
      {segments.map((segment, idx) => {
        const segmentWidth = (segment.value / total) * 100;
        return (
          <div
            key={segment.label || idx}
            style={{
              width: `${segmentWidth}%`,
              backgroundColor: segment.color,
            }}
          />
        );
      })}
    </div>
  );
};

export default StackedProgress;
