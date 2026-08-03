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
import { Button, Typography } from 'antd';
import { semanticColors, spacing, textStyles } from '../../theme/tokens';
import { NetworkErrorArt, NotFoundArt, ServerErrorArt } from './illustrations';

export interface ErrorStateProps {
  /** Illustration rendered above the title (e.g. one of the bundled error arts). */
  illustration?: React.ReactNode;
  /** Bold headline, e.g. "Network Error". */
  title: React.ReactNode;
  /** Supporting explanation shown under the title. */
  description?: React.ReactNode;
  /** Primary action label. Defaults to "Refresh". Pass `null` to hide the button. */
  actionLabel?: string | null;
  /** Action handler. Defaults to reloading the page. */
  onAction?: () => void;
  style?: React.CSSProperties;
}

/**
 * Full-page error / empty state: a centred illustration, headline, description and
 * a primary action button. Used for the network / 404 / 500 screens and any other
 * "nothing to show" state. Presets ({@link NetworkErrorState}, {@link NotFoundState},
 * {@link ServerErrorState}) fill in the art and copy from the design.
 */
export const ErrorState: React.FC<ErrorStateProps> = ({
  illustration,
  title,
  description,
  actionLabel = 'Refresh',
  onAction,
  style,
}) => {
  const handleAction = () => {
    if (onAction) {
      onAction();
    } else {
      window.location.reload();
    }
  };

  return (
    <div
      role="alert"
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        textAlign: 'center',
        gap: spacing.lg,
        minHeight: 320,
        padding: spacing.xxl,
        ...style,
      }}
    >
      {illustration}
      <div style={{ display: 'flex', flexDirection: 'column', gap: spacing.sm, alignItems: 'center' }}>
        <Typography.Title
          level={2}
          style={{
            margin: 0,
            fontSize: textStyles.h2.fontSize,
            lineHeight: `${textStyles.h2.lineHeight}px`,
            fontWeight: textStyles.h2.fontWeight,
            color: semanticColors.textPrimary,
          }}
        >
          {title}
        </Typography.Title>
        {description && (
          <Typography.Text
            style={{
              fontSize: textStyles.bodyLarge.fontSize,
              lineHeight: `${textStyles.bodyLarge.lineHeight}px`,
              color: semanticColors.textSecondary,
              maxWidth: 420,
            }}
          >
            {description}
          </Typography.Text>
        )}
      </div>
      {actionLabel && (
        <Button type="primary" onClick={handleAction}>
          {actionLabel}
        </Button>
      )}
    </div>
  );
};

/** "Network Error" state — no response received from the server. */
export const NetworkErrorState: React.FC<Omit<ErrorStateProps, 'title' | 'illustration'>> = (
  props
) => (
  <ErrorState
    illustration={<NetworkErrorArt />}
    title="Network Error"
    description="No response received from server while fetching data"
    {...props}
  />
);

/** "Error 404" state — the requested page/route does not exist. */
export const NotFoundState: React.FC<Omit<ErrorStateProps, 'title' | 'illustration'>> = (props) => (
  <ErrorState
    illustration={<NotFoundArt />}
    title="Error 404"
    description="The page is not available at the moment"
    {...props}
  />
);

/** "Error 500" state — the server hit an internal error. */
export const ServerErrorState: React.FC<Omit<ErrorStateProps, 'title' | 'illustration'>> = (
  props
) => (
  <ErrorState
    illustration={<ServerErrorArt />}
    title="Error 500"
    description="It’s not you, it’s us. We’re having an internal server error"
    {...props}
  />
);

export default ErrorState;
