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

import React, { useMemo, useState } from 'react';
import { Button, Dropdown, message, type MenuProps, type TableColumnsType } from 'antd';
import { DownOutlined } from '@ant-design/icons';
import { Card, Chip, DataTable, Icon, KeyValuePair, Section, SearchInput } from '@ozone-ui/shared';
import {
  JMX_QUERY,
  buildJvmHighlights,
  parseJvmArguments,
  toSystemPropertyRows,
  type JvmParameter,
  type JvmParameterCategory,
  type RuntimeBean,
} from '../../../api/overview';
import { useJmxBean } from '../../../api/useJmx';
import SectionBody from '../SectionBody';
import type { SectionProps } from './InstanceDetailsSection';

const highlightsGridStyle: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
  gap: '16px 24px',
};

const categoryColor: Record<JvmParameterCategory, 'blue' | 'orange' | 'neutral'> = {
  'System & Framework': 'blue',
  'Memory & GC': 'orange',
  'System Property': 'neutral',
};

const monospace: React.CSSProperties = {
  fontFamily: "'Roboto Mono', monospace",
  fontSize: 12,
};

const escapeXml = (s: string) =>
  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

/** Render parameter rows as a Hadoop-style XML configuration snippet. */
const buildConfigXml = (params: JvmParameter[]): string => {
  const body = params
    .map(
      (p) =>
        `  <property>\n    <name>${escapeXml(p.parameter)}</name>\n    <value>${escapeXml(
          p.value
        )}</value>\n  </property>`
    )
    .join('\n');
  return `<configuration>\n${body}\n</configuration>`;
};

const columns: TableColumnsType<JvmParameter> = [
  {
    title: 'Parameter',
    dataIndex: 'parameter',
    key: 'parameter',
    width: '34%',
    ellipsis: true,
    render: (parameter: string) => <span style={monospace}>{parameter}</span>,
  },
  {
    title: 'Value',
    dataIndex: 'value',
    key: 'value',
    width: '40%',
    ellipsis: true,
    render: (value: string) => <span style={monospace}>{value}</span>,
  },
  {
    title: 'Category',
    dataIndex: 'category',
    key: 'category',
    width: '26%',
    render: (category: JvmParameterCategory) => (
      <Chip color={categoryColor[category]} size="small">
        {category}
      </Chip>
    ),
  },
];

/**
 * "Java Virtual Machine" section: a Highlights card plus the searchable,
 * filterable and paginated Parameters table. Sourced from the JVM runtime bean,
 * fetched lazily only when this section renders.
 */
export const JvmSection: React.FC<SectionProps> = ({ refreshToken }) => {
  const {
    data: runtime,
    loading,
    error,
  } = useJmxBean<RuntimeBean>(JMX_QUERY.runtime, refreshToken);

  const [search, setSearch] = useState('');
  const [category, setCategory] = useState<'All' | JvmParameterCategory>('All');
  const [showModules, setShowModules] = useState(false);
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);

  const highlights = useMemo(() => (runtime ? buildJvmHighlights(runtime) : []), [runtime]);

  const allRows = useMemo<JvmParameter[]>(() => {
    if (!runtime) {
      return [];
    }
    const args = parseJvmArguments(runtime.InputArguments);
    return showModules ? [...args, ...toSystemPropertyRows(runtime.SystemProperties)] : args;
  }, [runtime, showModules]);

  const rows = useMemo(() => {
    const needle = search.trim().toLowerCase();
    return allRows.filter((row) => {
      if (category !== 'All' && row.category !== category) {
        return false;
      }
      if (!needle) {
        return true;
      }
      return (
        row.parameter.toLowerCase().includes(needle) || row.value.toLowerCase().includes(needle)
      );
    });
  }, [allRows, category, search]);

  const categoryOptions = [
    { label: 'All', value: 'All' },
    { label: 'System & Framework', value: 'System & Framework' },
    { label: 'Memory & GC', value: 'Memory & GC' },
    ...(showModules ? [{ label: 'System Property', value: 'System Property' }] : []),
  ];

  const categoryMenu: MenuProps = {
    items: categoryOptions.map((o) => ({ key: o.value, label: o.label })),
    selectable: true,
    selectedKeys: [category],
    onClick: ({ key }) => setCategory(key as 'All' | JvmParameterCategory),
  };

  // Copy the selected rows (or all filtered rows when none are selected) as a
  // Hadoop-style XML configuration snippet. Selection resolves against the full
  // row set so it survives search/category filtering.
  const copyArguments = async () => {
    const chosen = selectedRowKeys.length
      ? allRows.filter((r) => selectedRowKeys.includes(r.key))
      : rows;
    if (!chosen.length) {
      return;
    }
    await navigator.clipboard.writeText(buildConfigXml(chosen));
    message.success(
      `Copied ${chosen.length} ${chosen.length === 1 ? 'parameter' : 'parameters'} as XML`
    );
  };

  return (
    <Section title="Java Virtual Machine">
      <SectionBody loading={loading} error={error} skeletonRows={4}>
        {runtime && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
            <Card title="Highlights">
              <div style={highlightsGridStyle}>
                {highlights.map((h) => (
                  <KeyValuePair key={h.key} label={h.label} value={h.value} tooltip={h.tooltip} />
                ))}
              </div>
            </Card>

            <DataTable<JvmParameter>
              title="Parameters"
              columns={columns}
              dataSource={rows}
              rowKey="key"
              size="middle"
              paginated
              defaultPageSize={10}
              rowSelection={{ selectedRowKeys, onChange: setSelectedRowKeys }}
              onRow={(record) => ({
                onClick: () =>
                  setSelectedRowKeys((keys) =>
                    keys.includes(record.key)
                      ? keys.filter((k) => k !== record.key)
                      : [...keys, record.key]
                  ),
                style: { cursor: 'pointer' },
              })}
              filters={
                <>
                  <SearchInput
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                    placeholder="Search..."
                    width={256}
                  />
                  <Dropdown menu={categoryMenu} trigger={['click']}>
                    <span style={{ cursor: 'pointer' }}>
                      <Chip color="neutral">
                        {category === 'All' ? 'All' : category}
                        <DownOutlined style={{ fontSize: 10 }} />
                      </Chip>
                    </span>
                  </Dropdown>
                  <Chip
                    color="neutral"
                    selected={showModules}
                    onClick={() => setShowModules((v) => !v)}
                    style={{ cursor: 'pointer' }}
                  >
                    Show JVM Modules
                  </Chip>
                </>
              }
              actions={
                <Button
                  icon={<Icon name="copy" size={16} />}
                  onClick={copyArguments}
                  aria-label="Copy JVM arguments"
                >
                  Copy Arguments
                </Button>
              }
            />
          </div>
        )}
      </SectionBody>
    </Section>
  );
};

export default JvmSection;
