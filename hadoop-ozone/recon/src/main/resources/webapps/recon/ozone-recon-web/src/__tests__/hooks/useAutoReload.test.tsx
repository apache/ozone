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

import React from 'react';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { vi } from 'vitest';
import { useAutoReload } from '@/v2/hooks/useAutoReload.hook';

function TestAutoReload({ refresh }: { refresh: () => void }) {
  const autoReload = useAutoReload(refresh, 1000);
  return <button onClick={() => autoReload.handleAutoReloadToggle(!autoReload.isPolling)}>
    {autoReload.isPolling ? 'Stop' : 'Start'}
  </button>;
}

describe('useAutoReload', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    sessionStorage.removeItem('autoReloadEnabled');
  });
  afterEach(() => {
    vi.useRealTimers();
    sessionStorage.removeItem('autoReloadEnabled');
  });

  test.each([null, 'true', 'false'])('loads once on mount with auto reload %s', (enabled) => {
    if (enabled !== null) {
      sessionStorage.setItem('autoReloadEnabled', enabled);
    }
    const refresh = vi.fn();
    const { unmount } = render(<TestAutoReload refresh={refresh} />);
    expect(refresh).toHaveBeenCalledTimes(1);
    act(() => { vi.advanceTimersByTime(1000); });
    expect(refresh).toHaveBeenCalledTimes(enabled === 'false' ? 1 : 2);
    unmount();
    act(() => { vi.advanceTimersByTime(2000); });
    expect(refresh).toHaveBeenCalledTimes(enabled === 'false' ? 1 : 2);
  });

  test('stops and resumes polling with the latest refresh callback', () => {
    const refresh = vi.fn();
    const latestRefresh = vi.fn();
    const { rerender, unmount } = render(<TestAutoReload refresh={refresh} />);
    fireEvent.click(screen.getByText('Stop'));
    act(() => { vi.advanceTimersByTime(1000); });
    expect(refresh).toHaveBeenCalledTimes(1);
    rerender(<TestAutoReload refresh={latestRefresh} />);
    fireEvent.click(screen.getByText('Start'));
    expect(latestRefresh).toHaveBeenCalledTimes(1);
    act(() => { vi.advanceTimersByTime(1000); });
    expect(latestRefresh).toHaveBeenCalledTimes(2);
    unmount();
    expect(vi.getTimerCount()).toBe(0);
  });
});
