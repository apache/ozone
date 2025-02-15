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

import React, { PureComponent } from 'react';
import {
  ActionMeta,
  default as ReactSelect,
  Props as ReactSelectProps,
  ValueType,
  components,
  ValueContainerProps, OptionProps
} from 'react-select';
import makeAnimated from 'react-select/animated';

export interface IOption {
  label: string;
  value: string;
}

interface IMultiSelectProps extends ReactSelectProps<IOption> {
  options: IOption[];
  allowSelectAll: boolean;
  allOption?: IOption;
  maxShowValues?: number;
}

const defaultProps = {
  allOption: {
    label: 'Select all',
    value: '*'
  }
};

export class MultiSelect extends PureComponent<IMultiSelectProps> {
  static defaultProps = defaultProps;
  render() {
    const { allowSelectAll, allOption, options, maxShowValues = 5, onChange } = this.props;
    if (allowSelectAll) {
      const Option = (props: OptionProps<IOption>) => {
        return (
          <div>
            <components.Option {...props}>
              <input
                type='checkbox'
                checked={props.isSelected}
                onChange={() => null}
              />{' '}
              <label>{props.label}</label>
            </components.Option>
          </div>
        );
      };

      const ValueContainer = ({ children, ...props }: ValueContainerProps<IOption>) => {
        const currentValues: IOption[] = props.getValue() as IOption[];
        let toBeRendered = children;
        if (currentValues.some(val => val.value === allOption!.value) && children) {
          toBeRendered = allOption!.label;
        } else if (currentValues.length > maxShowValues) {
          toBeRendered = `${currentValues.length} selected`;
        }

        return (
          <components.ValueContainer {...props}>
            {toBeRendered}
          </components.ValueContainer>
        );
      };

      const animatedComponents = makeAnimated();
      return (
        <ReactSelect
          {...this.props}
          options={[allOption!, ...options]}
          components={{
            Option,
            ValueContainer,
            animatedComponents
          }}
          onChange={(selected: ValueType<IOption>, event: ActionMeta<IOption>) => {
            const selectedValues = selected as IOption[];
            if (selectedValues && selectedValues.length > 0) {
              if (selectedValues[selectedValues.length - 1].value === allOption!.value) {
                return onChange!([allOption!, ...options], { action: 'select-option' });
              }

              let result: IOption[] = [];
              if (selectedValues.length === options.length) {
                if (selectedValues.some(option => option.value === allOption!.value)) {
                  result = selectedValues.filter(
                    option => option.value !== allOption!.value
                  );
                } else if (event.action === 'select-option') {
                  result = [allOption!, ...options];
                }

                return onChange!(result, { action: 'select-option' });
              }
            }

            return onChange!(selected, { action: 'select-option' });
          }}
        />
      );
    }

    return <ReactSelect {...this.props} />;
  }
}
