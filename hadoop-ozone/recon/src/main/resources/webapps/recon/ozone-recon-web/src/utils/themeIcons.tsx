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

const fmtKB = (bytes?: number) =>
  typeof bytes === 'number' ? `${Math.round(bytes / 1024)}k` : undefined;
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

export interface IReplicationIconProps {
  className?: string;
  replicationType: "RATIS" | "STAND_ALONE" | "EC";

  // For RATIS
  replicationFactor?: "ONE" | "TWO" | "THREE";
  isLeader?: boolean;
  leaderNode?: string;

  // For EC
  ecData?: number;
  ecParity?: number;
  ecChunkSize?: number;
  ecCodec?: string;
}
export class RatisIcon extends React.PureComponent<{
  replicationFactor?: string;
  isLeader?: boolean;
}> {
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

export class ECIcon extends React.PureComponent {
  render() {
    return (
      <div className='ec-icon'>
        <div className='icon-label suffix-none'>EC</div>
      </div>
    );
  }
}

// Tooltip for per type
class RatisTip extends React.PureComponent<Pick<IReplicationIconProps, "replicationFactor" | "leaderNode">> {
  render() {
    const { replicationFactor, leaderNode } = this.props;
    return (
      <div>
        <div>Replication Type: RATIS</div>
        {replicationFactor ? <div>Replication Factor: {replicationFactor}</div> : null}
        {leaderNode ? <div>Leader Node: {leaderNode}</div> : null}
      </div>
    )
  }
}

class StandaloneTip extends React.PureComponent {
  render() {
    return (
      <div>
        <div>Replication Type: STAND_ALONE</div>
      </div>
    )
  }
}

class ECTip extends React.PureComponent<Pick<IReplicationIconProps, "ecCodec" | "ecData" | "ecParity" | "ecChunkSize">> {
  static defaultProps = { ecCodec: "RS"};
  render() {
    const { ecCodec = 'RS', ecData, ecParity, ecChunkSize } = this.props;
    const parts = [ecCodec, ecData?.toString(), ecParity?.toString(), fmtKB(ecChunkSize)].filter(Boolean) as string[];
    return (
      <div>
        <div>Replication Type: EC</div>
        <div>Codec: {ecCodec}</div>
        {typeof ecData === "number" && typeof ecParity === "number" ? (
          <div>Data/Parity: {ecData}+{ecParity}</div>
        ) : null}
        {typeof ecChunkSize === "number" ? <div>Chunk Size: {fmtKB(ecChunkSize)}</div> : null}
        {parts.length >= 3 ? <div>Layout: {parts.join('-')}</div> : null}
      </div>
    )
  }
}

export class ReplicationIcon extends React.PureComponent<IReplicationIconProps> {
  render() {
    const { replicationType, className } = this.props;
    // Assign icons only for RATIS and STAND_ALONE types
    let icon = null;
    let tip = null;

    if (replicationType === 'RATIS') {
      icon = (
        <RatisIcon
          replicationFactor={this.props.replicationFactor}
          isLeader={this.props.isLeader}
        />
      );
      tip = (
        <RatisTip
          replicationFactor={this.props.replicationFactor}
          leaderNode={this.props.leaderNode}
        />
      )
    } else if (replicationType === 'STAND_ALONE') {
      icon = <StandaloneIcon />;
      tip = <StandaloneTip />;
    } else if (replicationType === 'EC') {
      icon = <ECIcon />;
      tip = (<ECTip
        ecCodec={this.props.ecCodec}
        ecData={this.props.ecData}
        ecParity={this.props.ecParity}
        ecChunkSize={this.props.ecChunkSize}
        />
      );
    }

    // Wrap the icon in a tooltip
    if (!icon) return null;

    return (
      <Tooltip title={tip} placement="right" className="pointer">
        <div className={`replication-icon ${className ?? ''}`.trim()}>{icon}</div>
      </Tooltip>
    );
  }
}

export default ReplicationIcon;