/**
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

import moment from 'moment';

/**
 * JMX MBean queries used by the Overview sections. Kept in one place so each
 * section references a query by name; sections that share a query (e.g. the OM
 * ServerRuntime bean) are de-duplicated to a single request by the JMX cache.
 */
export const JMX_QUERY = {
  /** OM ServerRuntime bean: RPC port, ratis roles, data dirs, version, build. */
  omInfo: 'Hadoop:service=*,name=*,component=ServerRuntime',
  /** This node's Ratis RaftServer bean: id, leader, role, group. */
  ratisServer: 'Ratis:service=RaftServer,group=*,id=*',
  /** JVM runtime bean: input arguments and system properties. */
  runtime: 'java.lang:type=Runtime',
  /**
   * Ratis leader-election metrics for the current node. The name patterns are
   * matched by the mock; a live cluster may need the node id/group interpolated
   * (e.g. `ratis:name=ratis.leader_election.<id>@<group>.electionCount`).
   */
  leaderElectionCount: 'ratis:name=ratis.leader_election.*electionCount',
  leaderElectionElapsed: 'ratis:name=ratis.leader_election.*lastLeaderElectionElapsedTime',
} as const;

/* --------------------------------- Beans ---------------------------------- */

export interface OzoneManagerInfoBean {
  RpcPort: string;
  RatisRoles: string;
  RatisLogDirectory: string;
  RocksDbDirectory: string;
  Version: string;
  SoftwareVersion: string;
  StartedTimeInMillis: number;
  CompileInfo: string;
}

export interface RatisServerBean {
  Id: string;
  LeaderId: string;
  Role: string;
  GroupId: string;
  CurrentTerm: number;
}

/** Ratis leader-election count metric (current node). */
export interface LeaderElectionCountBean {
  Count: number;
}

/** Ratis last-leader-election elapsed-time metric in milliseconds (current node). */
export interface LeaderElectionElapsedBean {
  Value: number;
}

export interface SystemProperty {
  key: string;
  value: string;
}

export interface RuntimeBean {
  VmName: string;
  VmVendor: string;
  VmVersion: string;
  Name: string;
  InputArguments: string[];
  SystemProperties: SystemProperty[];
}

/* ------------------------------ View models ------------------------------- */

export interface KeyValue {
  key: string;
  label: string;
  value: string;
  copyable?: boolean;
  tooltip?: string;
}

export type RatisRoleName = 'LEADER' | 'FOLLOWER' | string;

export interface RatisRole {
  key: string;
  hostName: string;
  nodeId: string;
  ratisPort: string;
  role: RatisRoleName;
  /** Derived follower sync state; `null` for the leader row. */
  readiness: 'Synced' | 'Lagging' | null;
  /** True for the node serving this JMX endpoint. */
  isCurrent: boolean;
}

export type JvmParameterCategory = 'System & Framework' | 'Memory & GC' | 'System Property';

export interface JvmParameter {
  key: string;
  parameter: string;
  value: string;
  category: JvmParameterCategory;
}

/* -------------------------------- Parsers --------------------------------- */

/**
 * Parse the OM `RatisRoles` string, e.g.
 * `{ HostName: h1 | Node-Id: om1 | Ratis-Port : 9872 | Role: FOLLOWER } {...}`.
 */
export function parseRatisRoles(raw: string, currentNodeId?: string): RatisRole[] {
  const groups = raw?.match(/\{[^}]*\}/g) ?? [];
  return groups.map((group, index) => {
    const fields: Record<string, string> = {};
    group
      .replace(/[{}]/g, '')
      .split('|')
      .forEach((part) => {
        const sep = part.indexOf(':');
        if (sep === -1) {
          return;
        }
        fields[part.slice(0, sep).trim()] = part.slice(sep + 1).trim();
      });
    const role = (fields.Role ?? '').toUpperCase();
    const nodeId = fields['Node-Id'] ?? '';
    return {
      key: nodeId || String(index),
      hostName: fields.HostName ?? '',
      nodeId,
      ratisPort: fields['Ratis-Port'] ?? '',
      role,
      // The leader has no "readiness"; followers are shown as synced with the leader.
      readiness: role === 'LEADER' ? null : 'Synced',
      isCurrent: !!currentNodeId && nodeId === currentNodeId,
    };
  });
}

const MEMORY_GC = /Xm[xsn]|Xss|gc|CMS|Heap|Memory/i;

function categorize(parameter: string): JvmParameterCategory {
  return MEMORY_GC.test(parameter) ? 'Memory & GC' : 'System & Framework';
}

/** Split a single JVM argument into a `{ parameter, value }` pair. */
function splitArgument(arg: string): { parameter: string; value: string } {
  if (arg.startsWith('-D')) {
    const eq = arg.indexOf('=');
    return eq === -1
      ? { parameter: arg, value: 'Present' }
      : { parameter: arg.slice(0, eq), value: arg.slice(eq + 1) };
  }
  if (arg.startsWith('-XX:')) {
    const body = arg.slice(4);
    if (body.startsWith('+')) {
      return { parameter: arg, value: 'Enabled' };
    }
    if (body.startsWith('-')) {
      return { parameter: arg, value: 'Disabled' };
    }
    const eq = body.indexOf('=');
    return eq === -1
      ? { parameter: arg, value: 'Present' }
      : { parameter: `-XX:${body.slice(0, eq)}`, value: body.slice(eq + 1) };
  }
  if (arg.startsWith('-Xloggc:')) {
    return { parameter: '-Xloggc', value: arg.slice('-Xloggc:'.length) };
  }
  if (/^-Xm[xsn]/.test(arg) || arg.startsWith('-Xss')) {
    return { parameter: arg.slice(0, 4), value: arg.slice(4) };
  }
  if (arg.startsWith('-verbose:')) {
    return { parameter: '-verbose', value: arg.slice('-verbose:'.length) };
  }
  return { parameter: arg, value: 'Present' };
}

/** Parse JVM `InputArguments` into categorised parameter rows. */
export function parseJvmArguments(args: string[]): JvmParameter[] {
  return (args ?? []).map((arg, index) => {
    const { parameter, value } = splitArgument(arg);
    return { key: `arg-${index}`, parameter, value, category: categorize(parameter) };
  });
}

/** Map JVM `SystemProperties` into parameter rows (for the "Show JVM Modules" toggle). */
export function toSystemPropertyRows(props: SystemProperty[]): JvmParameter[] {
  return (props ?? []).map((prop, index) => ({
    key: `prop-${index}`,
    parameter: prop.key,
    value: prop.value === '' ? '—' : prop.value,
    category: 'System Property',
  }));
}

function formatHeap(xmx: string | undefined): string {
  if (!xmx) {
    return 'Not set';
  }
  const match = xmx.slice(4).match(/^(\d+)\s*([kKmMgG])?/);
  if (!match) {
    return xmx.slice(4);
  }
  const size = Number(match[1]);
  const unit = (match[2] ?? 'B').toUpperCase();
  const megabytes = unit === 'G' ? size * 1024 : unit === 'K' ? Math.round(size / 1024) : size;
  return `${megabytes.toLocaleString('en-US')} MB`;
}

function detectGarbageCollector(args: string[]): string {
  const flags = args.join(' ');
  if (/UseG1GC/.test(flags)) {
    return 'G1GC';
  }
  if (/UseConcMarkSweepGC/.test(flags)) {
    return 'ConcMarkSweep (CMS)';
  }
  if (/UseParallelGC/.test(flags)) {
    return 'Parallel';
  }
  if (/UseZGC/.test(flags)) {
    return 'ZGC';
  }
  if (/UseShenandoahGC/.test(flags)) {
    return 'Shenandoah';
  }
  return 'Default';
}

export interface JvmHighlight {
  key: string;
  label: string;
  value: string;
  tooltip: string;
}

/** Build the JVM "Highlights" key-value pairs from the runtime bean. */
export function buildJvmHighlights(runtime: RuntimeBean): JvmHighlight[] {
  const props = new Map(runtime.SystemProperties.map((p) => [p.key, p.value]));
  const args = runtime.InputArguments ?? [];
  const runtimeName = props.get('java.runtime.name') ?? runtime.VmName;
  const javaVersion = props.get('java.version') ?? runtime.VmVersion;
  const gcPause = args.find((a) => a.startsWith('-XX:MaxGCPauseMillis='));
  return [
    {
      key: 'runtime',
      label: 'Runtime Environment',
      value: `${runtimeName} ${javaVersion}`.trim(),
      tooltip: `${runtime.VmName} (${runtime.VmVendor})`,
    },
    {
      key: 'heap',
      label: 'Max Heap Memory',
      value: formatHeap(args.find((a) => a.startsWith('-Xmx'))),
      tooltip: 'Configured JVM maximum heap size (-Xmx).',
    },
    {
      key: 'gc',
      label: 'Garbage Collector',
      value: detectGarbageCollector(args),
      tooltip: 'Active garbage collector, detected from JVM flags.',
    },
    {
      key: 'gcPause',
      label: 'GC Pause Target',
      value: gcPause ? `${gcPause.split('=')[1]} ms` : 'Not set',
      tooltip: 'Target max GC pause (-XX:MaxGCPauseMillis), when configured.',
    },
  ];
}

/** Format an epoch-millis timestamp the way the Overview cards display it. */
export function formatStarted(millis: number): string {
  return moment(millis).format('MMM D, YYYY h:mm:ss A');
}

/**
 * Format an elapsed duration (milliseconds) as e.g. "2 days 3 hours",
 * "12 hours 40 mins", or "5 mins". Returns "—" for missing/negative input.
 */
export function formatElapsed(millis: number | undefined): string {
  if (!millis || millis < 0) {
    return '—';
  }
  const totalMinutes = Math.floor(millis / 60000);
  const days = Math.floor(totalMinutes / 1440);
  const hours = Math.floor((totalMinutes % 1440) / 60);
  const mins = totalMinutes % 60;
  const unit = (n: number, name: string) => `${n} ${name}${n === 1 ? '' : 's'}`;
  if (days > 0) {
    return `${unit(days, 'day')} ${unit(hours, 'hour')}`;
  }
  if (hours > 0) {
    return `${unit(hours, 'hour')} ${unit(mins, 'min')}`;
  }
  return unit(mins, 'min');
}
