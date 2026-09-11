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

import React from 'react';
import { Select, Typography } from 'antd';
import {
  DoubleLeftOutlined,
  DoubleRightOutlined,
  LeftOutlined,
  RightOutlined,
} from '@ant-design/icons';
import { semanticColors, spacing, textStyles } from '../../theme/tokens';
import IconButton from '../IconButton/IconButton';

export interface TablePaginationProps {
  /** Current page (1-based). */
  current: number;
  /** Rows per page. */
  pageSize: number;
  /** Total number of rows across all pages. */
  total: number;
  /** Selectable page sizes. Defaults to `[10, 20, 50, 100]`. */
  pageSizeOptions?: number[];
  /** Called when the page changes. */
  onChange: (page: number) => void;
  /** Called when the page size changes. */
  onPageSizeChange?: (pageSize: number) => void;
  style?: React.CSSProperties;
}

const labelStyle: React.CSSProperties = {
  color: semanticColors.textSecondary,
  fontSize: textStyles.bodySmall.fontSize,
  lineHeight: `${textStyles.bodySmall.lineHeight}px`,
};

/**
 * Data-table pagination footer. Renders a rows-per-page selector, a
 * "Showing X-Y of Z" range summary and first/previous/next/last navigation with
 * a page selector, matching the "Data Table Pagination" component in the mocks.
 */
export const TablePagination: React.FC<TablePaginationProps> = ({
  current,
  pageSize,
  total,
  pageSizeOptions = [10, 20, 50, 100],
  onChange,
  onPageSizeChange,
  style,
}) => {
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const from = total === 0 ? 0 : (current - 1) * pageSize + 1;
  const to = Math.min(current * pageSize, total);

  const goTo = (page: number) => {
    const next = Math.min(Math.max(1, page), pageCount);
    if (next !== current) {
      onChange(next);
    }
  };

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: spacing.lg,
        paddingBlock: spacing.md,
        paddingInline: spacing.xl,
        ...style,
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: spacing.sm }}>
        <Typography.Text style={labelStyle}>Rows per page:</Typography.Text>
        <Select
          size="small"
          value={pageSize}
          onChange={(value) => onPageSizeChange?.(value)}
          options={pageSizeOptions.map((v) => ({ label: String(v), value: v }))}
          style={{ width: 72 }}
        />
      </div>

      <Typography.Text style={labelStyle}>{`Showing ${from}-${to} of ${total}`}</Typography.Text>

      <div style={{ display: 'flex', alignItems: 'center', gap: spacing.xs }}>
        <IconButton
          size="standard"
          icon={<DoubleLeftOutlined />}
          label="First page"
          disabled={current <= 1}
          onClick={() => goTo(1)}
        />
        <IconButton
          size="standard"
          icon={<LeftOutlined />}
          label="Previous page"
          disabled={current <= 1}
          onClick={() => goTo(current - 1)}
        />
        <Select
          size="small"
          value={current}
          onChange={goTo}
          options={Array.from({ length: pageCount }, (_, i) => ({
            label: String(i + 1),
            value: i + 1,
          }))}
          style={{ width: 64 }}
        />
        <Typography.Text style={labelStyle}>{`of ${pageCount} pages`}</Typography.Text>
        <IconButton
          size="standard"
          icon={<RightOutlined />}
          label="Next page"
          disabled={current >= pageCount}
          onClick={() => goTo(current + 1)}
        />
        <IconButton
          size="standard"
          icon={<DoubleRightOutlined />}
          label="Last page"
          disabled={current >= pageCount}
          onClick={() => goTo(pageCount)}
        />
      </div>
    </div>
  );
};

export default TablePagination;
