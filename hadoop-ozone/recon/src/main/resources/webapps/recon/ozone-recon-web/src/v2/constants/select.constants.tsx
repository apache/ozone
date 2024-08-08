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

import { StylesConfig } from "react-select";
import { Option } from "@/v2/components/select/multiSelect";

export const selectStyles: StylesConfig<Option, true> = {
  control: (baseStyles, state) => ({
    ...baseStyles,
    minWidth: 200,
    boxShadow: 'none',
    borderRadius: '2px',
    borderColor: state.isFocused ? '#1AA57A' : '#E6E6E6',
    '&:hover': {
      borderColor: '#1AA57A'
    }
  }),
  option: (baseStyles, state) => ({
    ...baseStyles,
    display: 'flex',
    padding: '5px 12px',
    alignItems: 'center',
    color: state.isSelected ? '#1AA57A' : '#262626',
    backgroundColor: state.isSelected ? '#EDF7F4' : '#FFFFFF',
    '&:active': {
      color: state.isSelected ? '#FFFFFF' : '#262626',
      backgroundColor: state.isSelected ? '#64BDA1' : '#EDF7F4'
    }
  }),
  menuList: (baseStyles) => ({
    ...baseStyles,
    boxShadow: 'rgba(50, 50, 93, 0.25) 0px 6px 12px -2px, rgba(0, 0, 0, 0.3) 0px 3px 7px -3px',
    padding: 0
  }),
  menu: (baseStyles) => ({
    ...baseStyles,
    height: 100
  }),
  placeholder: (baseStyles) => ({
    ...baseStyles,
    color: 'rgba(0, 0, 0, 0.85)'

  }),
  indicatorSeparator: () => ({
    display: 'none'
  })
}
