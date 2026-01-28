# Hadoop UI Monorepo

This monorepo contains three React applications for Apache Ozone HDDS UI components:

## Applications

- **Recon** - Data node management and monitoring
- **SCM** - Storage Container Manager interface  
- **OM** - Ozone Manager interface

## Structure

```
hadoop-ui/src/
├── packages/
│   ├── shared/           # Shared components and utilities
│   ├── recon/           # Recon application
│   ├── scm/             # SCM application
│   └── om/              # OM application
├── package.json         # Root package.json with workspace configuration
└── pnpm-workspace.yaml  # PNPM workspace configuration
```

## Development

### Prerequisites

- Node.js >= 20.0.0 (Node 20 LTS recommended)
- PNPM >= 8.0.0

### Installation

```bash
# Install all dependencies
pnpm install
```

### Development

```bash
# Start development server for a specific app
pnpm dev:recon
pnpm dev:scm
pnpm dev:om

# Build shared components (run this first if you make changes to shared)
pnpm build:shared
```

### Building

```bash
# Build all applications
pnpm build

# Build specific application
pnpm build:recon
pnpm build:scm
pnpm build:om

# Build only shared components
pnpm build:shared
```

Build outputs are placed in:
- `build/recon/` - Recon application build
- `build/scm/` - SCM application build  
- `build/om/` - OM application build

### Clean

```bash
# Clean all build artifacts and node_modules
pnpm clean
```

## Architecture

### Shared Components

The `@hadoop-ui/shared` package contains:

- **Components**: Reusable React components (e.g., Sidebar)
- **Utils**: Shared utility functions (e.g., menu utilities)
- **Types**: TypeScript type definitions

### Individual Applications

Each application (`recon`, `scm`, `om`) is a standalone Vite + React + TypeScript application that can import from the shared package.

## Technology Stack

- **Build Tool**: Vite
- **Framework**: React 18
- **Language**: TypeScript
- **UI Library**: Ant Design v5
- **Package Manager**: PNPM (with workspaces)
- **Monorepo**: PNPM Workspaces 