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

import React from "react";
import {
  default as ReactSelect,
  Props as ReactSelectProps,
  components,
  OptionProps,
  ValueType,
  ValueContainerProps,
  StylesConfig
} from 'react-select';

import { selectStyles } from "@/v2/constants/select.constants";


// ------------- Types -------------- //
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
  style?: StylesConfig<Option, true>;
  onChange: (arg0: ValueType<Option, true>) => void;
  onTagClose: (arg0: string) => void;
}

// ------------- Component -------------- //

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
          onChange={() => null}
          disabled={props.isDisabled} />
        <label>{props.label}</label>
      </components.Option>
    </div>
  )
}


const MultiSelect: React.FC<MultiSelectProps> = ({
  options = [],
  selected = [],
  maxSelected = 5,
  placeholder = 'Columns',
  isDisabled = false,
  fixedColumn,
  columnLength,
  tagRef,
  style,
  onTagClose = () => { },  // Assign default value as a void function
  onChange = () => { },  // Assign default value as a void function
  ...props
}) => {

  const ValueContainer = ({ children, ...props }: ValueContainerProps<Option, true>) => {
    return (
      <components.ValueContainer {...props}>
        {React.Children.map(children, (child) => (
          ((child as React.ReactElement<any, string
            | React.JSXElementConstructor<any>>
            | React.ReactPortal)?.type as React.JSXElementConstructor<any>)).name === "DummyInput"
          ? child
          : null
        )}
        {isDisabled
          ? placeholder
          : `${placeholder}: ${selected.length} selected`
}
      </components.ValueContainer>
    );
  };

  const finalStyles = {...selectStyles, ...style ?? {}}

  return (
    <ReactSelect
      {...props}
      isMulti={true}
      closeMenuOnSelect={false}
      hideSelectedOptions={false}
      isClearable={false}
      isSearchable={false}
      controlShouldRenderValue={false}
      classNamePrefix='multi-select'
      options={options.map((opt) => ({...opt, isDisabled: (opt.value === fixedColumn)}))}
      components={{
        ValueContainer,
        Option
      }}
      placeholder={placeholder}
      value={selected}
      isDisabled={isDisabled}
      onChange={(selected: ValueType<Option, true>) => {
        if (selected?.length === options.length) return onChange!(options);
        return onChange!(selected);
      }}
      styles={finalStyles} />
  )
}

export default MultiSelect;
