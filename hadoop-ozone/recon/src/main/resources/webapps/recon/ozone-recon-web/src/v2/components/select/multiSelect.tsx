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

import React, { useCallback, useMemo, useRef, useState } from "react";
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
  showSearch?: boolean;
  showSelectAll?: boolean;
  onChange: (arg0: ValueType<Option, true>) => void;
  onTagClose: (arg0: string) => void;
}

// ------------- Component -------------- //

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
  // Read by the stable InputComponent closure to decide whether to suppress blur.
  const searchInteracting = useRef(false);
  // Ref to the outer container div — used to detect "focus left the widget".
  const containerRef = useRef<HTMLDivElement>(null);

  const fixedOption = fixedColumn ? options.find((opt) => opt.value === fixedColumn) : undefined;
  const selectableOptions = fixedColumn ? options.filter((opt) => opt.value !== fixedColumn) : options;

  // Always-current values for use inside stable useMemo components.
  const stateRef = useRef({
    searchTerm,
    setSearchTerm,
    showSearch,
    showSelectAll,
    selected,
    options,
    selectableOptions,
    fixedOption,
    onChange,
    setIsMenuOpen,
    containerRef
  });
  stateRef.current = {
    searchTerm,
    setSearchTerm,
    showSearch,
    showSelectAll,
    selected,
    options,
    selectableOptions,
    fixedOption,
    onChange,
    setIsMenuOpen,
    containerRef
  };

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
          : `${placeholder}: ${selected.filter((opt) => opt.value !== fixedColumn).length} selected`
        }
      </components.ValueContainer>
    );
  };

  // Stable custom Input — suppresses react-select's blur-driven menu close
  // while the user interacts with the search box inside the menu.
  const InputComponent = useMemo(
    () => (({ onBlur, ...inputProps }: any) => {
      const handleBlur = (e: React.FocusEvent<HTMLElement>) => {
        if (searchInteracting.current) return;
        if (onBlur) onBlur(e);
      };
      return <input {...inputProps} onBlur={handleBlur} />;
    }) as React.FC,
    [] // searchInteracting captured by ref — always current
  );

  // Stable MenuList — created once, reads current values from stateRef at
  // call time to avoid stale closures without re-creating the component type.
  const MenuListComponent = useMemo(
    () => ({ children, ...menuListProps }: any) => {
      const {
        searchTerm,
        setSearchTerm,
        showSearch,
        showSelectAll,
        selected,
        selectableOptions,
        fixedOption,
        options,
        onChange
      } = stateRef.current;

      const allSelected = selectableOptions.length > 0 &&
        selectableOptions.every((opt: Option) => selected.some((s: Option) => s.value === opt.value));

      const handleSelectAll = () => {
        onChange(fixedOption ? [fixedOption, ...selectableOptions] : selectableOptions);
      };

      const handleUnselectAll = () => {
        onChange(fixedOption ? [fixedOption] : ([] as Option[]));
      };

      return (
        <components.MenuList {...menuListProps}>
          {showSearch && (
            <div
              style={{ padding: '8px 12px' }}
              onMouseDown={(e) => {
                searchInteracting.current = true;
                e.preventDefault();
              }}
              onClick={() => {
                const input = (e: any) => e?.target?.closest('[data-search-wrapper]')?.querySelector('input');
                const el = document.querySelector('[data-search-wrapper] input') as HTMLInputElement | null;
                if (el) el.focus();
              }}
              onFocus={() => { searchInteracting.current = true; }}
              onBlur={() => {
                searchInteracting.current = false;
                setTimeout(() => {
                  const container = stateRef.current.containerRef.current;
                  if (container && !container.contains(document.activeElement)) {
                    stateRef.current.setIsMenuOpen(false);
                    stateRef.current.setSearchTerm('');
                  }
                }, 150);
              }}
              onKeyDown={(e) => {
                if (e.key === 'Escape') {
                  searchInteracting.current = false;
                  stateRef.current.setIsMenuOpen(false);
                  stateRef.current.setSearchTerm('');
                }
                e.stopPropagation();
              }}
              data-search-wrapper='true'
            >
              <input
                type='text'
                placeholder='Search...'
                value={searchTerm}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)}
                onClick={(e: React.MouseEvent) => e.stopPropagation()}
                style={{
                  width: '100%',
                  padding: '6px 8px',
                  borderRadius: '4px',
                  border: '1px solid #d9d9d9',
                  fontSize: '14px',
                  boxSizing: 'border-box',
                  outline: 'none'
                }}
              />
            </div>
          )}
          {showSelectAll && (
            <div
              style={{
                padding: '6px 12px',
                cursor: 'pointer',
                borderBottom: '1px solid #f0f0f0',
                display: 'flex',
                alignItems: 'center'
              }}
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => allSelected ? handleUnselectAll() : handleSelectAll()}
            >
              <input
                type='checkbox'
                checked={allSelected}
                onChange={() => null}
                style={{ marginRight: '8px', accentColor: '#1AA57A' }}
              />
              <label style={{ cursor: 'pointer' }}>
                {allSelected ? 'Unselect All' : 'Select All'}
              </label>
            </div>
          )}
          {children}
        </components.MenuList>
      );
    },
    [] // Stable reference — reads current values from stateRef
  );

  // Only intercept onMenuClose when showSearch is active so we can keep
  // the dropdown open while the user interacts with the search box.
  const handleMenuClose = useCallback(() => {
    if (!searchInteracting.current) {
      setIsMenuOpen(false);
      setSearchTerm('');
    }
  }, []);

  const searchModeProps = showSearch
    ? {
      menuIsOpen: isMenuOpen,
      onMenuOpen: () => setIsMenuOpen(true),
      onMenuClose: handleMenuClose
    }
    : {};

  const finalStyles = { ...selectStyles, ...style ?? {} };

  const select = (
    <ReactSelect
      {...props}
      {...(searchModeProps as any)}
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
        MenuList: MenuListComponent,
        ...(showSearch ? { Input: InputComponent } : {})
      }}
      menuPortalTarget={document.body}
      placeholder={placeholder}
      value={selected.filter((opt) => opt.value !== fixedColumn)}
      isDisabled={isDisabled}
      onChange={(selectedValue: ValueType<Option, true>) => {
        const selectedOpts = (selectedValue as Option[]) ?? [];
        const withFixed = fixedOption ? [fixedOption, ...selectedOpts] : selectedOpts;
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
