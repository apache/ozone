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

import React, { Children } from "react";
import Select, {
  Props as ReactSelectProps,
  components,
  ValueType,
  ValueContainerProps,
  StylesConfig
} from 'react-select';

import { selectStyles } from "@/v2/constants/select.constants";


export type Option = {
  label: string;
  value: string;
}

interface SingleSelectProps extends ReactSelectProps<Option, false> {
  options: Option[];
  placeholder: string;
  onChange: (arg0: ValueType<Option, false>) => void;
}

const SingleSelect: React.FC<SingleSelectProps> = ({
  options = [],
  placeholder = 'Limit',
  onChange = () => { },  // Assign default value as a void function
  ...props  // Desctructure other select props
}) => {


  const ValueContainer = ({ children, ...props }: ValueContainerProps<Option, false>) => {
    const selectedLimit = props.getValue() as Option[];
    return (
      <components.ValueContainer {...props}>
        {Children.map(children, (child) => (
          !props.hasValue || 
          (child as React.ReactElement<any, string
            | React.JSXElementConstructor<any>>
            | React.ReactPortal)!.type === components.Input
          ? child
          : null
        ))}
        Limit: {selectedLimit[0]?.label ?? ''}
      </components.ValueContainer>
    );
  };

  return (
    <Select
      {...props}
      isClearable={false}
      closeMenuOnSelect={true}
      isSearchable={false}
      classNamePrefix='single-select'
      options={options}
      components={{
        ValueContainer
      }}
      placeholder={placeholder}
      onChange={(selected: ValueType<Option, false>) => {
        return onChange!(selected);
      }}
      styles={selectStyles as StylesConfig<Option, false>} />
  );
}

export default SingleSelect;
