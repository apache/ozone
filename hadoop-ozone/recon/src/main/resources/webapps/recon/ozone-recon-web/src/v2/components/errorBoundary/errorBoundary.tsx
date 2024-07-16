import React from "react";

type ErrorProps = {
  fallback: string | React.ReactNode;
  children: React.ReactNode;
}

type ErrorState = {
  hasError: boolean;
}

class ErrorBoundary extends React.Component<ErrorProps, ErrorState>{
  constructor(props: ErrorProps) {
    super(props);
    this.state = { hasError: false }
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true }
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    console.error(error, errorInfo)
  }

  render(): React.ReactNode {
    if (this.state.hasError) {
      return this.props.fallback;
    }
    return this.props.children;
  }
}

export default ErrorBoundary;