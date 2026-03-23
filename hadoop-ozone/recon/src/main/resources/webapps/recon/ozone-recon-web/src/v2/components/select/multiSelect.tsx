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

import React, { useMemo, useRef, useState } from "react";
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
import MultiSelectMenuList from './multiSelectMenuList';


// ------------- Types -------------- //
export type Option = {
  label: string;
  value: string;
}

interface MultiSelectProps extends ReactSelectProps<Option, true> {
  options: Option[];
  selected: Option[];
  placeholder: string;
  // Accept a single key or an array of keys for columns that are always
  // selected, hidden from the dropdown, and preserved through Unselect All.
  fixedColumn: string | string[];
  columnLength: number;
  style?: StylesConfig<Option, true>;
  showSearch?: boolean;
  showSelectAll?: boolean;
  onChange: (arg0: ValueType<Option, true>) => void;
  onTagClose: (arg0: string) => void;
}

// ------------- Module-level sub-components -------------- //

const OptionComponent: React.FC<OptionProps<Option, true>> = (props) => {
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
  );
};

// Defined at module level so the reference is stable — no useMemo required.
// Suppresses react-select's blur-driven menu close while the user interacts
// with the search box inside the menu.
// searchInteracting ref is passed through react-select's selectProps.
const MultiSelectInput = ({ onBlur, ...inputProps }: any) => {
  const { searchInteracting } = inputProps.selectProps ?? {};
  const handleBlur = (e: React.FocusEvent<HTMLElement>) => {
    if (searchInteracting?.current) return;
    if (onBlur) onBlur(e);
  };
  return <input {...inputProps} onBlur={handleBlur} />;
};

// ------------- Component -------------- //

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
  showSearch = false,
  showSelectAll = false,
  onTagClose = () => { },
  onChange = () => { },
  ...props
}) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [isMenuOpen, setIsMenuOpen] = useState(false);

  // True while the user's pointer/keyboard focus is inside the search wrapper.
  // Passed via selectProps so MultiSelectInput and MultiSelectMenuList can
  // suppress react-select's blur-driven close without a stale closure.
  const searchInteracting = useRef(false);
  // Ref to the outer container div — used to detect "focus left the widget".
  const containerRef = useRef<HTMLDivElement>(null);

  // Normalise fixedColumn to an array of keys for uniform handling.
  const fixedKeys: string[] = Array.isArray(fixedColumn)
    ? fixedColumn.filter(Boolean)
    : fixedColumn ? [fixedColumn] : [];

  const fixedOptions = options.filter((opt) => fixedKeys.includes(opt.value));
  const selectableOptions = options.filter((opt) => !fixedKeys.includes(opt.value));

  const filteredOptions = useMemo(() => {
    if (!showSearch || !searchTerm) return selectableOptions;
    return selectableOptions.filter((opt) =>
      opt.label.toLowerCase().includes(searchTerm.toLowerCase())
    );
  }, [options, searchTerm, showSearch]);

  const ValueContainer = ({ children, ...vcProps }: ValueContainerProps<Option, true>) => {
    return (
      <components.ValueContainer {...vcProps}>
        {React.Children.map(children, (child) => (
          ((child as React.ReactElement<any, string
            | React.JSXElementConstructor<any>>
            | React.ReactPortal)?.type as React.JSXElementConstructor<any>)).name === "DummyInput"
          ? child
          : null
        )}
        {isDisabled
          ? placeholder
          : `${placeholder}: ${selected.filter((opt) => !fixedKeys.includes(opt.value)).length} selected`
        }
      </components.ValueContainer>
    );
  };

  // Plain function — setters and refs are stable so no useCallback is needed.
  // Guard against closing while the user is interacting with the search box.
  // react-select fires onMenuClose when its DummyInput blurs, which also
  // happens when we programmatically focus the search input (step in onClick).
  // The searchInteracting ref blocks that false-positive close.
  // For genuine outside clicks while the search box is focused, the race
  // condition means this guard temporarily wins, but the 150ms onBlur
  // fallback in MultiSelectMenuList closes the menu shortly after.
  const handleMenuClose = () => {
    if (!searchInteracting.current) {
      setIsMenuOpen(false);
      setSearchTerm('');
    }
  };

  const searchModeProps = showSearch
    ? {
      menuIsOpen: isMenuOpen,
      onMenuOpen: () => setIsMenuOpen(true),
      onMenuClose: handleMenuClose
    }
    : {};

  const finalStyles = { ...selectStyles, ...style ?? {} };

  // Extra data passed via selectProps so the module-level MultiSelectMenuList
  // and MultiSelectInput components can read current state without closures.
  // customOnChange is renamed to avoid shadowing react-select's own onChange.
  const menuListProps = {
    searchTerm,
    setSearchTerm,
    showSearch,
    showSelectAll,
    selected,
    selectableOptions,
    fixedOptions,
    customOnChange: onChange,
    searchInteracting,
    setIsMenuOpen,
    containerRef
  };

  const select = (
    <ReactSelect
      {...props}
      {...(searchModeProps as any)}
      {...(menuListProps as any)}
      isMulti={true}
      closeMenuOnSelect={false}
      hideSelectedOptions={false}
      isClearable={false}
      isSearchable={false}
      controlShouldRenderValue={false}
      classNamePrefix='multi-select'
      options={filteredOptions}
      components={{
        ValueContainer,
        Option: OptionComponent,
        MenuList: MultiSelectMenuList,
        ...(showSearch ? { Input: MultiSelectInput } : {})
      }}
      menuPortalTarget={document.body}
      placeholder={placeholder}
      value={selected.filter((opt) => !fixedKeys.includes(opt.value))}
      isDisabled={isDisabled}
      onChange={(selectedValue: ValueType<Option, true>) => {
        const selectedOpts = (selectedValue as Option[]) ?? [];
        const withFixed = [...fixedOptions, ...selectedOpts];
        if (selectedOpts.length === selectableOptions.length) return onChange!(options);
        return onChange!(withFixed);
      }}
      styles={finalStyles} />
  );

  // Wrap in a container div only when showSearch is active so we have a
  // boundary for detecting "focus left the widget" in the search onBlur.
  return showSearch
    ? <div ref={containerRef}>{select}</div>
    : select;
};

export default MultiSelect;
