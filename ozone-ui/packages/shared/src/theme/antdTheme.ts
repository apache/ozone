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

import type { ThemeConfig } from 'antd';
import { colors, fontFamilies, radius, semanticColors, spacing, textStyles } from './tokens';

/**
 * Ant Design v5 theme derived from the Ozone UI design tokens.
 *
 * Pass this to `<ConfigProvider theme={ozoneTheme}>` (see `ThemeProvider`) so
 * every Ant Design component picks up the design-system colours, typography and
 * radii without per-component overrides.
 */
export const ozoneTheme: ThemeConfig = {
  token: {
    // Brand / status colours
    colorPrimary: semanticColors.brand,
    colorInfo: semanticColors.info,
    colorSuccess: semanticColors.success,
    colorWarning: semanticColors.warning,
    colorError: semanticColors.error,
    colorLink: semanticColors.info,

    // Neutrals / text
    colorText: semanticColors.textPrimary,
    colorTextSecondary: semanticColors.textSecondary,
    colorTextTertiary: semanticColors.textTertiary,
    colorTextDisabled: semanticColors.textDisabled,
    colorBorder: semanticColors.border,
    colorBorderSecondary: semanticColors.border,
    colorBgLayout: semanticColors.bgLayout,
    colorBgContainer: semanticColors.bgContainer,
    colorBgElevated: semanticColors.bgElevated,
    colorFillSecondary: semanticColors.fill,

    // Typography
    fontFamily: fontFamilies.base,
    fontFamilyCode: fontFamilies.monospace,
    fontSize: textStyles.bodyStandard.fontSize,
    fontSizeSM: textStyles.bodySmall.fontSize,
    fontSizeLG: textStyles.bodyLarge.fontSize,
    fontSizeHeading1: textStyles.h1.fontSize,
    fontSizeHeading2: textStyles.h2.fontSize,
    fontSizeHeading3: textStyles.h3.fontSize,
    fontSizeHeading4: textStyles.h4.fontSize,
    fontSizeHeading5: textStyles.h5.fontSize,
    lineHeight: textStyles.bodyStandard.lineHeight / textStyles.bodyStandard.fontSize,

    // Shape
    borderRadius: radius.md,
    borderRadiusLG: radius.lg,
    borderRadiusSM: radius.sm,

    // Control sizing kept aligned with the 4px spacing rhythm.
    controlHeight: 32,
    wireframe: false,
  },
  components: {
    Layout: {
      headerBg: semanticColors.bgTopbar,
      headerColor: semanticColors.textPrimary,
      headerHeight: 56,
      headerPadding: '0 24px',
      bodyBg: semanticColors.bgLayout,
      // The navigation rail shares the light layout surface.
      siderBg: semanticColors.bgSidebar,
      // Collapse control shares the rail surface so it doesn't read as a
      // separate section (the Sidebar renders its own left-aligned trigger).
      triggerBg: semanticColors.bgSidebar,
      triggerColor: semanticColors.navItemColor,
    },
    Menu: {
      // Light rail: no full-row selection fill — a left accent bar (applied per
      // item in the Sidebar) marks the active item instead.
      itemBg: 'transparent',
      subMenuItemBg: 'transparent',
      itemColor: semanticColors.navItemColor,
      itemSelectedBg: 'transparent',
      itemSelectedColor: semanticColors.navItemColorSelected,
      itemHoverBg: semanticColors.navItemBgHover,
      itemHoverColor: semanticColors.navItemColorHover,
      itemActiveBg: semanticColors.navItemBgHover,
      groupTitleColor: semanticColors.navGroupTitleColor,
      // Radius/margins are applied per item in the Sidebar so the hover/selection
      // pill rounds on the right only (the left edge carries the accent bar).
      itemBorderRadius: 0,
      itemMarginInline: 0,
      itemMarginBlock: spacing.xs,
      // Suppress Ant Design's built-in inline selection border.
      activeBarWidth: 0,
      activeBarBorderWidth: 0,
    },
    Card: {
      borderRadiusLG: radius.lg,
      colorBorderSecondary: semanticColors.border,
      headerFontSize: textStyles.h3.fontSize,
      paddingLG: 20,
    },
    Button: {
      borderRadius: radius.md,
      fontWeight: 500,
      primaryShadow: 'none',
      defaultShadow: 'none',
    },
    Tag: {
      borderRadiusSM: radius.pill,
      defaultBg: colors.pewter[50],
      defaultColor: semanticColors.textSecondary,
    },
    Table: {
      headerBg: colors.pewter[25],
      headerColor: semanticColors.textSecondary,
      borderColor: semanticColors.border,
      rowHoverBg: colors.pewter[25],
    },
    Input: {
      borderRadius: radius.md,
      colorBorder: semanticColors.borderStrong,
    },
    Alert: {
      borderRadiusLG: radius.md,
    },
  },
};

export default ozoneTheme;
