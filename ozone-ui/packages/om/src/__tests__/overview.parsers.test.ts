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

import { describe, expect, it } from 'vitest';
import {
  buildJvmHighlights,
  formatElapsed,
  formatStarted,
  parseJvmArguments,
  parseRatisRoles,
  toSystemPropertyRows,
  type RuntimeBean,
} from '../api/overview';

describe('parseRatisRoles', () => {
  const rows = [
    ['host-a.example.com', 'om-a', '9872', 'FOLLOWER', 'LEADER_AND_READY'],
    ['host-b.example.com', 'om-b', '9872', 'LEADER', 'LEADER_AND_READY'],
  ];

  it('maps [host, nodeId, port, role, readiness] tuples', () => {
    const [a, b] = parseRatisRoles(rows);
    expect(a).toMatchObject({
      hostName: 'host-a.example.com',
      nodeId: 'om-a',
      ratisPort: '9872',
      role: 'FOLLOWER',
      readiness: 'Synced',
      isCurrent: false,
    });
    // The leader row has no readiness.
    expect(b).toMatchObject({ role: 'LEADER', readiness: null });
  });

  it('flags the current node by id', () => {
    const parsed = parseRatisRoles(rows, 'om-b');
    expect(parsed.find((r) => r.isCurrent)?.nodeId).toBe('om-b');
    expect(parsed.filter((r) => r.isCurrent)).toHaveLength(1);
  });

  it('uppercases the role', () => {
    const [role] = parseRatisRoles([['h', 'n', '9872', 'follower']]);
    expect(role.role).toBe('FOLLOWER');
  });

  it('skips malformed rows such as the single-element error row', () => {
    const parsed = parseRatisRoles([['No leader found in the cluster'], rows[0]]);
    expect(parsed).toHaveLength(1);
    expect(parsed[0].nodeId).toBe('om-a');
  });

  it('returns an empty array for undefined input (no throw)', () => {
    expect(parseRatisRoles(undefined)).toEqual([]);
  });
});

describe('parseJvmArguments', () => {
  it('splits the supported argument shapes', () => {
    const rows = parseJvmArguments([
      '-Dproc_om',
      '-Dhdp.version=7.3.2',
      '-XX:+UseG1GC',
      '-XX:-UseParallelGC',
      '-XX:MaxGCPauseMillis=200',
      '-Xmx4096m',
      '-Xss256k',
      '-Xloggc:/var/log/gc.log',
      '-verbose:gc',
      'com.example.Main',
    ]);
    expect(rows.map(({ parameter, value }) => ({ parameter, value }))).toEqual([
      { parameter: '-Dproc_om', value: 'Present' },
      { parameter: '-Dhdp.version', value: '7.3.2' },
      { parameter: '-XX:+UseG1GC', value: 'Enabled' },
      { parameter: '-XX:-UseParallelGC', value: 'Disabled' },
      { parameter: '-XX:MaxGCPauseMillis', value: '200' },
      { parameter: '-Xmx', value: '4096m' },
      { parameter: '-Xss', value: '256k' },
      { parameter: '-Xloggc', value: '/var/log/gc.log' },
      { parameter: '-verbose', value: 'gc' },
      { parameter: 'com.example.Main', value: 'Present' },
    ]);
  });

  it('categorizes memory/GC flags separately from system flags', () => {
    const rows = parseJvmArguments(['-Xmx4096m', '-XX:+UseG1GC', '-Dproc_om']);
    expect(rows.map((r) => r.category)).toEqual([
      'Memory & GC',
      'Memory & GC',
      'System & Framework',
    ]);
  });

  it('returns an empty array for undefined input', () => {
    expect(parseJvmArguments(undefined as unknown as string[])).toEqual([]);
  });
});

describe('toSystemPropertyRows', () => {
  it('maps properties and renders blank values as a dash', () => {
    const rows = toSystemPropertyRows([
      { key: 'java.version', value: '17.0.11' },
      { key: 'sun.arch.data.model', value: '' },
    ]);
    expect(rows).toEqual([
      { key: 'prop-0', parameter: 'java.version', value: '17.0.11', category: 'System Property' },
      { key: 'prop-1', parameter: 'sun.arch.data.model', value: '—', category: 'System Property' },
    ]);
  });
});

describe('buildJvmHighlights', () => {
  const runtime = (args: string[], props: Record<string, string> = {}): RuntimeBean => ({
    VmName: 'OpenJDK 64-Bit Server VM',
    VmVendor: 'Eclipse Adoptium',
    VmVersion: '17.0.11+9',
    Name: '123@host',
    InputArguments: args,
    SystemProperties: Object.entries(props).map(([key, value]) => ({ key, value })),
  });

  const heapValue = (args: string[]) =>
    buildJvmHighlights(runtime(args)).find((h) => h.key === 'heap')?.value;

  it('formats heap sizes with explicit units', () => {
    expect(heapValue(['-Xmx4g'])).toBe('4,096 MB');
    expect(heapValue(['-Xmx4096m'])).toBe('4,096 MB');
    expect(heapValue(['-Xmx524288k'])).toBe('512 MB');
  });

  it('treats a bare -Xmx value as bytes (the reported bug)', () => {
    // 2,511,000,000 bytes / 1024² ≈ 2,395 MB — not "2,511,000,000 MB".
    expect(heapValue(['-Xmx2511000000'])).toBe('2,395 MB');
  });

  it('reports "Not set" when no -Xmx flag is present', () => {
    expect(heapValue(['-XX:+UseG1GC'])).toBe('Not set');
  });

  it('detects the garbage collector and GC pause target', () => {
    const highlights = buildJvmHighlights(runtime(['-XX:+UseG1GC', '-XX:MaxGCPauseMillis=200']));
    expect(highlights.find((h) => h.key === 'gc')?.value).toBe('G1GC');
    expect(highlights.find((h) => h.key === 'gcPause')?.value).toBe('200 ms');
  });

  it('builds the runtime-environment label from system properties', () => {
    const highlights = buildJvmHighlights(
      runtime([], { 'java.runtime.name': 'OpenJDK Runtime Environment', 'java.version': '17.0.11' })
    );
    expect(highlights.find((h) => h.key === 'runtime')?.value).toBe(
      'OpenJDK Runtime Environment 17.0.11'
    );
  });
});

describe('formatStarted', () => {
  it('formats an epoch-millis timestamp as a readable date', () => {
    // Assert the shape rather than an exact string to stay timezone-independent.
    expect(formatStarted(1785178223133)).toMatch(
      /^[A-Z][a-z]{2} \d{1,2}, \d{4} \d{1,2}:\d{2}:\d{2} [AP]M$/
    );
  });
});

describe('formatElapsed', () => {
  it('returns a dash for missing or negative input', () => {
    expect(formatElapsed(undefined)).toBe('—');
    expect(formatElapsed(-1)).toBe('—');
  });

  it('formats minutes, hours and days with correct pluralization', () => {
    expect(formatElapsed(1 * 60_000)).toBe('1 min');
    expect(formatElapsed(5 * 60_000)).toBe('5 mins');
    expect(formatElapsed((2 * 60 + 40) * 60_000)).toBe('2 hours 40 mins');
    expect(formatElapsed(25 * 60 * 60_000)).toBe('1 day 1 hour');
    expect(formatElapsed(48 * 60 * 60_000)).toBe('2 days 0 hours');
  });
});
