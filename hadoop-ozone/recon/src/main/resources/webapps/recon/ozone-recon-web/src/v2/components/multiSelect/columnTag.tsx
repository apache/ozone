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
import { Tag } from "antd";
import { createPortal } from "react-dom";

export type TagProps = {
  label: string;
  closable: boolean;
  tagRef: React.RefObject<HTMLDivElement>;
  onClose: (arg0: string) => void;
}

const ColumnTag: React.FC<TagProps> = ({
  label = '',
  closable = true,
  tagRef = null,
  onClose = () => {}
}) => {
  const onPreventMouseDown = (event: React.MouseEvent<HTMLSpanElement>) => {
    event.preventDefault();
    event.stopPropagation();
  };

  const handleClose = (label: string) => {
    return onClose(label);
  }

  if (!tagRef?.current) return null;

  return createPortal(
    <Tag
      key={label}
      onMouseDown={onPreventMouseDown}
      closable={closable}
      onClose={() => handleClose(label)}
      style={{marginRight: 3}}>
        {label}
    </Tag>,
    tagRef.current
  );
}

export default ColumnTag;
