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

import React, { useEffect, useMemo, useState } from 'react';
import { Table, Typography, type TableProps } from 'antd';
import { radius, semanticColors, spacing, textStyles } from '../../theme/tokens';
import Icon from '../Icon/Icon';
import TablePagination from './TablePagination';

export interface DataTableProps<T> extends Omit<TableProps<T>, 'pagination' | 'title'> {
  /** Table title shown in the header bar. */
  title?: React.ReactNode;
  /** Filter controls rendered on the left of the toolbar row (search, chips, ...). */
  filters?: React.ReactNode;
  /** Action controls rendered on the right of the toolbar row (buttons, ...). */
  actions?: React.ReactNode;
  /** Show the custom pagination footer and paginate `dataSource` client-side. */
  paginated?: boolean;
  /** Initial rows per page when `paginated`. Defaults to 10. */
  defaultPageSize?: number;
  /** Selectable page sizes when `paginated`. */
  pageSizeOptions?: number[];
}

/**
 * Themed data table. Wraps Ant Design's `Table` with an optional header bar
 * (title + a filter/actions toolbar) and the design-system `TablePagination`
 * footer. When `paginated` is set the table paginates `dataSource` client-side;
 * otherwise all rows are shown. All standard `Table` props are supported.
 */
export function DataTable<T extends object>({
  title,
  filters,
  actions,
  paginated = false,
  defaultPageSize = 10,
  pageSizeOptions,
  dataSource,
  style,
  expandable,
  ...rest
}: DataTableProps<T>) {
  const [current, setCurrent] = useState(1);
  const [pageSize, setPageSize] = useState(defaultPageSize);

  // Design-system row expander: a chevron that flips right→down (matching the
  // "Icon-Only Expander" in the mocks). Applied by default when the caller
  // enables `expandable`; callers may still override `expandIcon`.
  const themedExpandIcon = ({
    expanded,
    expandable: rowExpandable,
    record,
    onExpand,
  }: {
    expanded: boolean;
    expandable: boolean;
    record: T;
    onExpand: (record: T, e: React.MouseEvent<HTMLElement>) => void;
  }) =>
    rowExpandable ? (
      <span
        role="button"
        tabIndex={0}
        aria-label={expanded ? 'Collapse row' : 'Expand row'}
        onClick={(e) => onExpand(record, e)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            onExpand(record, e as unknown as React.MouseEvent<HTMLElement>);
          }
        }}
        style={{ display: 'inline-flex', cursor: 'pointer', color: semanticColors.textSecondary }}
      >
        <Icon name={expanded ? 'chevron-down' : 'chevron-right'} size={16} />
      </span>
    ) : (
      <span style={{ display: 'inline-block', width: 16 }} />
    );

  const mergedExpandable = expandable ? { expandIcon: themedExpandIcon, ...expandable } : undefined;

  const rows = useMemo(() => dataSource ?? [], [dataSource]);
  const total = rows.length;

  // Return to the first page whenever the row set changes (e.g. search/filter),
  // so the visible page never falls out of range.
  useEffect(() => {
    setCurrent(1);
  }, [dataSource]);

  const pageRows = useMemo(() => {
    if (!paginated) {
      return rows;
    }
    const start = (current - 1) * pageSize;
    return rows.slice(start, start + pageSize);
  }, [paginated, rows, current, pageSize]);

  const hasHeader = title != null || filters != null || actions != null;

  return (
    <div
      style={{
        border: `1px solid ${semanticColors.border}`,
        borderRadius: radius.lg,
        background: semanticColors.bgContainer,
        overflow: 'hidden',
        ...style,
      }}
    >
      {hasHeader && (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            gap: spacing.lg,
            padding: spacing.xl,
            paddingBottom: filters || actions ? spacing.lg : spacing.xl,
          }}
        >
          {title != null && (
            <Typography.Title
              level={4}
              style={{
                margin: 0,
                fontSize: textStyles.h3.fontSize,
                lineHeight: `${textStyles.h3.lineHeight}px`,
                fontWeight: textStyles.h3.fontWeight,
                color: semanticColors.textPrimary,
              }}
            >
              {title}
            </Typography.Title>
          )}
          {(filters || actions) && (
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: spacing.lg,
              }}
            >
              <div
                style={{ display: 'flex', alignItems: 'center', gap: spacing.sm, flexWrap: 'wrap' }}
              >
                {filters}
              </div>
              {actions && (
                <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>
                  {actions}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      <Table<T> dataSource={pageRows} pagination={false} expandable={mergedExpandable} {...rest} />

      {paginated && (
        <div style={{ borderTop: `1px solid ${semanticColors.border}` }}>
          <TablePagination
            current={current}
            pageSize={pageSize}
            total={total}
            pageSizeOptions={pageSizeOptions}
            onChange={setCurrent}
            onPageSizeChange={(size) => {
              setPageSize(size);
              setCurrent(1);
            }}
          />
        </div>
      )}
    </div>
  );
}

export default DataTable;
