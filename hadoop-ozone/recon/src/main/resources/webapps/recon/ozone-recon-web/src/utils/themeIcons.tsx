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

import * as React from 'react';
import { Tooltip } from 'antd';

export class FilledIcon extends React.Component {
  render() {
    const path =
      'M864 64H160C107 64 64 107 64 160v' +
      '704c0 53 43 96 96 96h704c53 0 96-43 96-96V16' +
      '0c0-53-43-96-96-96z';
    return (
      <svg {...(this.props as Record<string, object>)} viewBox='0 0 1024 1024'>
        <path d={path} />
      </svg>
    );
  }
}

interface IRatisIconProps {
  replicationFactor: string;
  isLeader: boolean;
}

interface IReplicationIconProps {
  replicationFactor: string;
  replicationType: string;
  leaderNode: string;
  isLeader: boolean;
}

export class RatisIcon extends React.PureComponent<IRatisIconProps> {
  render() {
    const { replicationFactor, isLeader } = this.props;
    const threeFactorClass = isLeader ? 'icon-text-three-dots-leader' : 'icon-text-three-dots';
    const textClass = replicationFactor === "THREE" ? threeFactorClass : 'icon-text-one-dot';
    return (
      <div className='ratis-icon'>
        <div className={textClass}>R</div>
      </div>
    );
  }
}

export class StandaloneIcon extends React.PureComponent {
  render() {
    return (
      <div className='standalone-icon'>
        <div className='icon-text-one-dot'>S</div>
      </div>
    );
  }
}

export class ReplicationIcon extends React.PureComponent<IReplicationIconProps> {
  render() {
    const { replicationType, replicationFactor, isLeader, leaderNode } = this.props;
    // Assign icons only for RATIS and STAND_ALONE types
    let icon = null;
    if (replicationType === 'RATIS') {
      icon = <RatisIcon replicationFactor={replicationFactor} isLeader={isLeader} />;
    } else if (replicationType === 'STAND_ALONE') {
      icon = <StandaloneIcon />;
    }

    // Wrap the icon in a tooltip
    if (icon) {
      const tooltip = (
        <div>
          <div>Replication Type: {replicationType}</div>
          <div>Replication Factor: {replicationFactor}</div>
          <div>Leader Node: {leaderNode}</div>
        </div>
      );
      icon = (
        <Tooltip title={tooltip} placement='right' className='pointer'>
          <div className='replication-icon'>{icon}</div>
        </Tooltip>
      );
    }

    return icon;
  }
}

interface IGraphLegendIconProps {
  color: string;
  height?: number;
};
export class GraphLegendIcon extends React.PureComponent<IGraphLegendIconProps> {
  render() {
    const { color, height = 14 } = this.props;

    return (
      <svg
      width="18"
      height={height}
      viewBox={`0 0 18 ${height}`}
      xmlns="http://www.w3.org/2000/svg"
      style={{ display: 'inline-block', verticalAlign: 'middle' }} // Optional: helps with alignment
    >
      <circle
        cx="6"
        cy="6"
        r="6" 
        fill={color} // Use the color prop for the fill
      />
    </svg>
    )
  }
};