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

/**
 * Ozone UI design tokens.
 *
 * The single source of truth for colour and typography across the Ozone UI
 * monorepo. Prefer referencing these tokens (or the derived Ant Design theme in
 * `antdTheme.ts`) over hard-coding raw colour/size values.
 */

/** Full colour ramps. The `*` marked step is each family's canonical value. */
export const colors = {
  pewter: {
    25: 'rgb(244, 247, 249)',
    50: 'rgb(225, 234, 240)',
    100: 'rgb(206, 219, 228)', // base
    200: 'rgb(184, 200, 212)',
    300: 'rgb(163, 181, 194)',
    400: 'rgb(133, 154, 168)',
    500: 'rgb(110, 131, 145)',
    600: 'rgb(86, 104, 117)',
    700: 'rgb(69, 81, 90)',
    800: 'rgb(51, 60, 67)',
    900: 'rgb(35, 43, 48)',
    950: 'rgb(26, 32, 36)',
  },
  blueNova: {
    25: 'rgb(249, 249, 255)',
    50: 'rgb(240, 240, 255)',
    100: 'rgb(220, 220, 252)',
    200: 'rgb(199, 199, 253)',
    300: 'rgb(172, 172, 252)',
    400: 'rgb(144, 144, 252)',
    500: 'rgb(115, 115, 250)',
    600: 'rgb(85, 85, 249)', // base
    700: 'rgb(59, 46, 219)',
    800: 'rgb(48, 34, 144)',
    900: 'rgb(28, 19, 78)',
    950: 'rgb(19, 13, 50)',
  },
  orange: {
    25: 'rgb(255, 244, 240)',
    50: 'rgb(255, 227, 214)',
    100: 'rgb(255, 205, 184)',
    200: 'rgb(255, 169, 133)',
    300: 'rgb(255, 126, 71)',
    400: 'rgb(255, 85, 13)', // base (brand primary)
    500: 'rgb(224, 67, 0)',
    600: 'rgb(179, 53, 0)',
    700: 'rgb(133, 39, 0)',
    800: 'rgb(92, 27, 0)',
    900: 'rgb(46, 14, 0)',
    950: 'rgb(20, 6, 0)',
  },
  red: {
    25: 'rgb(255, 248, 248)',
    50: 'rgb(254, 234, 234)',
    100: 'rgb(254, 216, 216)',
    200: 'rgb(253, 192, 191)',
    300: 'rgb(252, 156, 155)',
    400: 'rgb(251, 115, 113)',
    500: 'rgb(247, 65, 59)',
    600: 'rgb(222, 36, 28)',
    700: 'rgb(181, 24, 16)', // base
    800: 'rgb(126, 13, 8)',
    900: 'rgb(74, 4, 3)',
    950: 'rgb(48, 2, 1)',
  },
  green: {
    25: 'rgb(240, 252, 241)',
    50: 'rgb(206, 245, 210)',
    100: 'rgb(171, 237, 180)',
    200: 'rgb(126, 229, 147)',
    300: 'rgb(68, 212, 104)',
    400: 'rgb(0, 189, 68)',
    500: 'rgb(0, 162, 57)',
    600: 'rgb(0, 138, 47)',
    700: 'rgb(0, 115, 38)', // base
    800: 'rgb(0, 78, 23)',
    900: 'rgb(0, 43, 9)',
    950: 'rgb(0, 27, 4)',
  },
  amber: {
    50: 'rgb(255, 245, 231)',
    100: 'rgb(255, 235, 204)',
    200: 'rgb(255, 213, 122)',
    300: 'rgb(247, 194, 0)', // base
    400: 'rgb(207, 162, 0)',
    500: 'rgb(172, 134, 0)',
    600: 'rgb(135, 105, 0)',
    700: 'rgb(103, 79, 0)',
    800: 'rgb(70, 53, 0)',
    900: 'rgb(41, 30, 0)',
  },
  pear: {
    50: 'rgb(236, 253, 195)',
    100: 'rgb(218, 251, 110)',
    200: 'rgb(196, 233, 36)',
    300: 'rgb(180, 214, 32)', // base (brand primary)
    400: 'rgb(151, 180, 25)',
    500: 'rgb(125, 149, 19)',
    600: 'rgb(97, 117, 12)',
    700: 'rgb(73, 88, 7)',
    800: 'rgb(48, 59, 3)',
    900: 'rgb(27, 34, 1)',
  },
} as const;

/**
 * Semantic colour aliases. UI code should prefer these over raw ramp steps so
 * intent is explicit and re-theming stays centralised.
 */
export const semanticColors = {
  brand: colors.green[500],
  brandHover: colors.green[400],
  brandActive: colors.green[600],
  info: colors.blueNova[600],
  success: colors.green[700],
  warning: colors.amber[300],
  error: colors.red[700],
  textPrimary: colors.pewter[900],
  textSecondary: colors.pewter[600],
  textTertiary: colors.pewter[500],
  textDisabled: colors.pewter[400],
  border: colors.pewter[100],
  borderStrong: colors.pewter[200],
  bgLayout: colors.pewter[25],
  bgContainer: 'rgb(255, 255, 255)',
  bgElevated: 'rgb(255, 255, 255)',
  fill: colors.pewter[50],
  skeleton: colors.pewter[50],
  // App chrome. The top utility bar and navigation rail share the light layout
  // surface so the whole shell reads as one continuous background.
  bgTopbar: colors.pewter[25],
  bgSidebar: colors.pewter[25],
  // Navigation rail item colours (light theme).
  navItemColor: colors.pewter[600],
  navItemColorSelected: colors.pewter[950],
  navItemColorHover: colors.pewter[900],
  navItemBgHover: colors.pewter[50],
  navIconColor: colors.pewter[400],
  navGroupTitleColor: colors.pewter[600],
  /** The 3px accent bar marking the selected navigation item (brand primary). */
  navIndicator: colors.green[500],
} as const;

/** Font families. Roboto is the primary UI face; app titles use Plus Jakarta Sans. */
export const fontFamilies = {
  base: "'Roboto', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
  monospace: "'Roboto Mono', 'SFMono-Regular', Consolas, 'Liberation Mono', monospace",
  appTitle: "'Plus Jakarta Sans', 'Roboto', sans-serif",
} as const;

export interface TextStyle {
  fontFamily: string;
  fontSize: number;
  fontWeight: number;
  lineHeight: number;
}

/** Type scale. Sizes/line-heights are in px. */
export const textStyles = {
  displayLarge: { fontFamily: fontFamilies.base, fontSize: 24, fontWeight: 600, lineHeight: 32 },
  h1: { fontFamily: fontFamilies.base, fontSize: 20, fontWeight: 600, lineHeight: 24 },
  h2: { fontFamily: fontFamilies.base, fontSize: 18, fontWeight: 600, lineHeight: 24 },
  h3: { fontFamily: fontFamilies.base, fontSize: 16, fontWeight: 600, lineHeight: 20 },
  h4: { fontFamily: fontFamilies.base, fontSize: 14, fontWeight: 600, lineHeight: 20 },
  h5: { fontFamily: fontFamilies.base, fontSize: 12, fontWeight: 600, lineHeight: 16 },
  h6: { fontFamily: fontFamilies.base, fontSize: 12, fontWeight: 600, lineHeight: 16 },
  bodyLarge: { fontFamily: fontFamilies.base, fontSize: 16, fontWeight: 400, lineHeight: 24 },
  bodyLargeBold: { fontFamily: fontFamilies.base, fontSize: 16, fontWeight: 600, lineHeight: 24 },
  bodyStandard: { fontFamily: fontFamilies.base, fontSize: 14, fontWeight: 400, lineHeight: 20 },
  bodyStandardBold: {
    fontFamily: fontFamilies.base,
    fontSize: 14,
    fontWeight: 600,
    lineHeight: 20,
  },
  bodySmall: { fontFamily: fontFamilies.base, fontSize: 12, fontWeight: 400, lineHeight: 16 },
  bodySmallBold: { fontFamily: fontFamilies.base, fontSize: 12, fontWeight: 600, lineHeight: 16 },
  monospaceStandard: {
    fontFamily: fontFamilies.monospace,
    fontSize: 14,
    fontWeight: 400,
    lineHeight: 20,
  },
  monospaceSmall: {
    fontFamily: fontFamilies.monospace,
    fontSize: 12,
    fontWeight: 400,
    lineHeight: 16,
  },
  appTitle: { fontFamily: fontFamilies.appTitle, fontSize: 16, fontWeight: 500, lineHeight: 24 },
} as const satisfies Record<string, TextStyle>;

/**
 * Spacing scale (px). A 4px-based scale aligned with Ant Design's default
 * sizing and the spacing rhythm used across the Ozone screens.
 */
export const spacing = {
  xxs: 2,
  xs: 4,
  sm: 8,
  md: 12,
  lg: 16,
  xl: 24,
  xxl: 32,
  xxxl: 48,
} as const;

/** Corner radii (px). */
export const radius = {
  sm: 4,
  md: 6,
  lg: 8,
  pill: 999,
} as const;

export type ColorFamily = keyof typeof colors;
export type TextStyleName = keyof typeof textStyles;
