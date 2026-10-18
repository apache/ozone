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

import React, { useRef } from 'react';
import { components } from 'react-select';


export type Option = {
  label: string;
  value: string;
}

// Intercepts react-select v3's onInputBlur to suppress menu-close while the
// user is interacting with the search box inside the MenuList.
export const MultiSelectInput: React.FC<any> = ({ onBlur, ...inputProps }) => {
  const { searchInteracting } = inputProps.selectProps;
  const handleBlur = (e: React.FocusEvent) => {
    if (searchInteracting?.current) return;
    if (onBlur) onBlur(e);
  };
  return <components.Input {...inputProps} onBlur={handleBlur} />;
};

// Custom MenuList for MultiSelect that renders an optional search box and
// Select All / Unselect All toggle above the option list.
// Defined as a standalone module-level component so react-select always
// receives a stable reference — no useMemo required.
// All state is passed through react-select's selectProps mechanism.
const MultiSelectMenuList = (props: any) => {
  const {
    searchTerm,
    setSearchTerm,
    showSearch,
    showSelectAll,
    selected,
    selectableOptions,
    fixedOptions,
    customOnChange,
    searchInteracting,
    setIsMenuOpen,
    containerRef
  } = props.selectProps;

  // Ref used to re-focus the input after e.preventDefault() on the wrapper's
  // onMouseDown (which suppresses the browser's default focus-on-click).
  const searchInputRef = useRef<HTMLInputElement>(null);

  const allSelected = selectableOptions?.length > 0 &&
    selectableOptions.every((opt: Option) =>
      selected?.some((s: Option) => s.value === opt.value)
    );

  const handleSelectAll = () => {
    customOnChange([...(fixedOptions ?? []), ...(selectableOptions ?? [])]);
  };

  const handleUnselectAll = () => {
    customOnChange(fixedOptions ?? []);
  };

  return (
    <components.MenuList {...props}>
      {showSearch && (
        <div
          style={{ padding: '8px 12px' }}
          onMouseDown={(e: React.MouseEvent) => {
            if (searchInteracting) searchInteracting.current = true;
            // Prevent react-select's DummyInput from receiving the blur that
            // would otherwise close the menu, but e.preventDefault() also
            // suppresses the browser's default focus-on-click for child inputs,
            // so we manually restore focus in onClick below.
            e.preventDefault();
          }}
          onClick={() => searchInputRef.current?.focus()}
          onFocus={() => {
            if (searchInteracting) searchInteracting.current = true;
          }}
          onBlur={() => {
            if (searchInteracting) searchInteracting.current = false;
            setTimeout(() => {
              const container = containerRef?.current;
              if (container && !container.contains(document.activeElement)) {
                setIsMenuOpen?.(false);
                setSearchTerm?.('');
              }
            }, 150);
          }}
          onKeyDown={(e: React.KeyboardEvent) => {
            if (e.key === 'Escape') {
              if (searchInteracting) searchInteracting.current = false;
              setIsMenuOpen?.(false);
              setSearchTerm?.('');
            }
            e.stopPropagation();
          }}
        >
          <input
            ref={searchInputRef}
            type='text'
            placeholder='Search...'
            value={searchTerm ?? ''}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
              setSearchTerm?.(e.target.value)
            }
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
          onMouseDown={(e: React.MouseEvent) => e.preventDefault()}
          onClick={() => allSelected ? handleUnselectAll() : handleSelectAll()}
        >
          <input
            type='checkbox'
            checked={allSelected ?? false}
            onChange={() => null}
            style={{ marginRight: '8px', accentColor: '#1AA57A' }}
          />
          <label style={{ cursor: 'pointer' }}>
            {allSelected ? 'Unselect All' : 'Select All'}
          </label>
        </div>
      )}
      {props.children}
    </components.MenuList>
  );
};

export default MultiSelectMenuList;
