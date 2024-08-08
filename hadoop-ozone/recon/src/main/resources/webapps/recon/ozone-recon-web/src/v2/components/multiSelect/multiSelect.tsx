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

import React, { useMemo } from "react";
import { createPortal } from "react-dom";
import {
  default as ReactSelect,
  Props as ReactSelectProps,
  components,
  OptionProps,
  ValueType
} from 'react-select';

import ColumnTag from "@/v2/components/multiSelect/columnTag";
import { selectStyles } from "@/v2/constants/select.constants";


export type Option = {
  label: string;
  value: string;
}

interface MultiSelectProps extends ReactSelectProps<Option, true> {
  options: Option[];
  selected: Option[];
  placeholder: string;
  fixedColumn: string;
  columnLength: number;
  parentRef: React.RefObject<HTMLDivElement>;
  tagRef: React.RefObject<HTMLDivElement>;
  onChange: (arg0: ValueType<Option, true>) => void;
  onTagClose: (arg0: string) => void;
}

function getColumnTags(
  options: Option[],
  tagRef: React.RefObject<HTMLDivElement>,
  fixedColumn: string,
  onClose: (arg0: string) => void) {
  let tagsEl: React.ReactElement[] = [];
  options.forEach((option) => {
    tagsEl.push(
      <ColumnTag
        tagRef={tagRef}
        label={option.label}
        closable={(option.label === fixedColumn) ? false : true}
        onClose={onClose} />
    )
  });
  return tagsEl;
}

const MultiSelect: React.FC<MultiSelectProps> = ({
  options = [],
  selected = [],
  maxSelected = 5,
  placeholder = 'Columns',
  fixedColumn,
  columnLength,
  parentRef,
  tagRef,
  onTagClose = () => { },
  onChange = () => { },
  ...props
}) => {

  const columnTags = useMemo(() =>
    getColumnTags(
      selected,
      tagRef,
      fixedColumn,
      onTagClose,
    ),
    [selected]
  );

  const Option: React.FC<OptionProps<Option, true>> = (props) => {
    return (
      <div>
        <components.Option
          {...props}>
          <input
            type='checkbox'
            checked={props.isSelected}
            style={{
              marginRight: '8px',
              accentColor: '#1AA57A'
            }}
            onChange={() => null} />
          <label>{props.label}</label>
        </components.Option>
      </div>
    )
  }


  if (!parentRef?.current) return null;

  return createPortal(
    <>
      <ReactSelect
        {...props}
        isMulti={true}
        closeMenuOnSelect={false}
        hideSelectedOptions={false}
        isClearable={false}
        isSearchable={false}
        controlShouldRenderValue={false}
        classNamePrefix='multi-select'
        options={options}
        components={{
          Option
        }}
        placeholder={placeholder}
        value={selected}
        onChange={(selected: ValueType<Option, true>) => {
          if (selected?.length === options.length) return onChange!(options);
          return onChange!(selected);
        }}
        styles={selectStyles} />
      {...columnTags}
    </>, parentRef.current
  );
}

export default MultiSelect;
