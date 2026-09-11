<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Ozone UI Monorepo

A Vite + React 18 + TypeScript monorepo (managed with **pnpm workspaces**) that
hosts the Apache Ozone web applications and a shared component library. Styling
is built on **Ant Design v5** themed with the Ozone UI design tokens.

## Applications & packages

| Package                 | Directory         | Description                                    |
| ----------------------- | ----------------- | ---------------------------------------------- |
| `@ozone-ui/shared`      | `packages/shared` | Design system: theme + reusable components     |
| `@ozone-ui/ozone-recon` | `packages/recon`  | Recon – datanode management and monitoring     |
| `@ozone-ui/ozone-scm`   | `packages/scm`    | SCM – Storage Container Manager interface      |
| `@ozone-ui/ozone-om`    | `packages/om`     | OM – Ozone Manager interface                   |

## Folder layout

```
ozone-ui/                         # pnpm workspace root
├── README.md
├── package.json                  # Root scripts + shared dev dependencies
├── pnpm-workspace.yaml           # Workspace globs (./packages/**)
├── pnpm-lock.yaml
├── tsconfig.json                 # Base TS config, extended by each package
├── vite.config.shared.ts         # Shared Vite config helpers for the apps
├── eslint.config.js              # Flat ESLint config
└── packages/
    ├── shared/                   # @ozone-ui/shared (design system)
    │   └── src/
    │       ├── theme/            # tokens, Ant Design theme, ThemeProvider
    │       ├── components/       # Sidebar, UtilityBar, PageHeader, Card, ...
    │       ├── utils/            # menuUtils, ...
    │       └── index.ts          # Public entry point (barrel)
    ├── recon/                    # @ozone-ui/ozone-recon (Vite app)
    ├── scm/                      # @ozone-ui/ozone-scm   (Vite app)
    └── om/                       # @ozone-ui/ozone-om    (Vite app)
        ├── mock/                 # json-server JMX mock (server.cjs, jmxData.cjs)
        └── src/
            ├── api/              # JMX client + section-driven data hooks/parsers
            ├── pages/            # Overview page + section components
            ├── navigation.tsx    # sidebar nav items
            └── App.tsx           # utility bar + sidebar + routes
```

## Prerequisites

- Node.js `>= 20` (Node 20 LTS recommended)
- pnpm `>= 8.15.7` (`corepack enable` provides the pinned version)

## Install

```bash
cd ozone-ui
pnpm install          # installs all workspace dependencies
```

## Develop

The `shared` package is consumed as a built artifact, so build it once (and
after any change to it) before/while running an app:

```bash
cd ozone-ui

pnpm build:shared      # compile @ozone-ui/shared -> packages/shared/dist

pnpm dev:recon         # start the Recon app dev server
pnpm dev:scm           # start the SCM app dev server
pnpm dev:om            # start the OM app dev server (http://localhost:3000)
```

> Tip: run `pnpm build:shared --watch` (or rebuild it after edits) whenever you
> change `@ozone-ui/shared`, since apps import the compiled `dist/` output.

## Mock backends (local development)

The apps talk to their Ozone service over HTTP. To develop without a live
cluster, each app can be paired with a **json-server** mock of its backend.
Mock commands are namespaced per service (`mock:om`, and later `mock:scm`,
`mock:recon`, …) so every sub-service can host its own mock independently.

### OM (Ozone Manager)

The OM app reads runtime state from the OM JMX servlet (`GET /jmx?qry=<mbean>`).
The mock in `packages/om/mock/` replays captured JMX responses on port `9878`;
the OM dev server proxies `/jmx` to it (see `packages/om/vite.config.ts`).

```bash
cd ozone-ui
pnpm build:shared      # once, and after any shared change

pnpm dev:om:mock       # OM mock (:9878) + OM dev server (:3000) together
# — or run them separately —
pnpm mock:om           # just the OM JMX mock on :9878
pnpm dev:om            # just the OM dev server on :3000
```

Then open http://localhost:3000. To point the app at a real OM instead of the
mock, change the `/jmx` proxy target in `packages/om/vite.config.ts` (or serve
the built app from the OM itself, where `/jmx` is same-origin).

## Build

```bash
cd ozone-ui

pnpm build             # build shared, then all three apps
pnpm build:recon       # build a single app
pnpm build:scm
pnpm build:om
pnpm build:shared      # build only the shared library
```

Application build output is written to `build/{recon,scm,om}/`.

## Lint & clean

```bash
pnpm lint              # ESLint across the workspace
pnpm clean             # remove build/ and all dist/ + node_modules
pnpm clean:cache       # clear Vite caches
pnpm clean:all         # clean + clean:cache
```

## Using the design system

Each app mounts the theme once near its root (already wired in
`packages/{om,scm,recon}/src/main.tsx`), then consumes shared components and
tokens from `@ozone-ui/shared`:

```tsx
import { BrowserRouter } from 'react-router-dom';
import {
  ThemeProvider,
  AppLayout,
  Sidebar,
  PageHeader,
  Card,
  KeyValuePair,
  Chip,
} from '@ozone-ui/shared';
import '@fontsource/roboto/400.css';
import '@fontsource/roboto/500.css';
import '@fontsource/roboto/700.css';

export default function App() {
  return (
    <BrowserRouter>
      <ThemeProvider>
        <AppLayout
          sider={
            <Sidebar
              logo={<span style={{ color: '#fff', padding: 12 }}>Ozone</span>}
              items={[{ key: 'overview', label: 'Overview', path: '/' }]}
            />
          }
        >
          <PageHeader title="Datanodes" subtitle="12 healthy" />
          <Card
            title="Instance details"
            emphasis="elevated"
            collapsible
            extra={<Chip color="green" variant="dot">Healthy</Chip>}
          >
            <KeyValuePair label="Hostname" value="dn-01.ozone.local" />
            <KeyValuePair label="UUID" value="a1b2c3" copyable />
          </Card>
        </AppLayout>
      </ThemeProvider>
    </BrowserRouter>
  );
}
```

> `Sidebar` is router-aware, so render it within a `react-router-dom` context
> (e.g. `BrowserRouter`); it highlights the active item and navigates on select.

### What's in `@ozone-ui/shared`

- **`theme/`**
  - `colors`, `semanticColors`, `textStyles`, `fontFamilies`, `spacing`,
    `radius` — design tokens (source of truth for colour and typography).
  - `ozoneTheme` — an Ant Design v5 `ThemeConfig` derived from the tokens.
  - `ThemeProvider` — wraps `ConfigProvider` with the theme and accepts optional
    per-app `themeOverrides`.
- **`components/`** (derived from the components recurring across the mockups)
  - `UtilityBar` — global top bar (leading/title, centre, actions).
  - `Sidebar` — collapsible, router-aware navigation rail driven by `items`
    (with `path`s, plus `group`/`divider` entries) and `logo` props; integrates
    with `react-router-dom`.
  - `AppLayout` — page shell with an optional full-width `utilityBar` slot above
    the sider + content row.
  - `PageHeader` — page title with breadcrumb, subtitle and actions.
  - `Section` — labelled content block: title, optional supporting text and
    actions, followed by its content.
  - `Card` — surface with `outlined`/`elevated`/`filled` emphasis and an
    optional `collapsible` header.
  - `KeyValuePair` — label/value pair (vertical or horizontal, optional
    link/copy and an info `tooltip`).
  - `DataTable` — themed Ant Design table with an optional title + filter/actions
    toolbar and a `TablePagination` footer (client-side paging via `paginated`).
  - `Chip` — pill: `full`/`dot` variant, `standard`/`small` size, colour and
    `selected`/`closable` states.
  - `SearchInput` — text field with a leading search glyph (table toolbars).
  - `Alert` — inline status banner (info/success/warning/error).
  - `TextLink` — themed inline link with optional external affordance.
  - `IconButton` — square icon-only button with accessible label + tooltip.
  - `Icon` — inline-SVG icon set (`currentColor`, tree-shakeable).

Prefer the tokens/theme over hard-coded colours or font sizes so re-theming
stays centralised.

## Technology stack

- **Build**: Vite 5 (apps), `tsc` (shared library)
- **Framework**: React 18
- **Language**: TypeScript 5.6
- **UI**: Ant Design v5, themed with the Ozone UI design tokens
- **Fonts**: Roboto / Roboto Mono (`@fontsource/roboto`), Plus Jakarta Sans (app titles)
- **Package manager / monorepo**: pnpm workspaces
