/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from 'react';
import { BrowserRouter } from 'react-router-dom';
import '@testing-library/react/dont-cleanup-after-each';
import { cleanup, render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { 
  assistantServer, 
  mockHealthDisabled, 
  mockHealthNotConfigured,
  mockChatBusy,
  mockChatTimeout,
  mockChatError,
  mockChatDisabled,
  mockChatInterrupted,
  mockHealthEnabled,
  mockModels,
  mockModelsError,
  mockModelsDisabled,
  mockChatSuccess,
  mockChatDelayed
} from '@tests/mocks/assistantMocks/assistantServer';
import Assistant from '@/v2/pages/assistant/assistant';
import { CHATBOT_ENDPOINTS } from '@/v2/constants/chatbot.constants';
import { rest } from 'msw';

const WrappedAssistantComponent = () => {
  return (
    <BrowserRouter>
      <Assistant />
    </BrowserRouter>
  )
}

describe('Assistant Tests', () => {
  afterEach(() => {
    assistantServer.resetHandlers();
    sessionStorage.clear();
    cleanup();
  });

  beforeAll(() => {
    assistantServer.listen();
  });

  afterAll(() => {
    assistantServer.close();
  });

  it('renders disabled state when health check returns enabled=false', async () => {
    assistantServer.use(mockHealthDisabled);
    render(<WrappedAssistantComponent />);
    
    await waitFor(() => {
      expect(screen.getByText('Recon AI is Disabled')).toBeVisible();
    });
  });

  it('renders not configured state when health check returns llmClientAvailable=false', async () => {
    assistantServer.use(mockHealthNotConfigured);
    render(<WrappedAssistantComponent />);
    
    await waitFor(() => {
      expect(screen.getByText('Recon AI is Not Configured')).toBeVisible();
    });
  });

  it('renders empty state when enabled and configured', async () => {
    assistantServer.use(mockHealthEnabled, mockModels);
    render(<WrappedAssistantComponent />);
    
    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });
    
    // Seed prompts should be visible
    expect(screen.getByText('How many unhealthy containers are there?')).toBeVisible();
  });

  it('sends a message and renders markdown response', async () => {
    assistantServer.use(mockHealthEnabled, mockModels, mockChatSuccess);
    render(<WrappedAssistantComponent />);
    
    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });

    // Type a message
    const input = screen.getByPlaceholderText('Ask Recon AI about your cluster...');
    await userEvent.type(input, 'Hello');
    
    // Click send
    const sendButton = screen.getByRole('button', { name: /send/i });
    await userEvent.click(sendButton);

    // Wait for the response
    await waitFor(() => {
      // Markdown bold should render as a strong tag
      expect(screen.getByText('Markdown')).toHaveStyle('font-weight: bold');
      // Table should render
      expect(screen.getByRole('table')).toBeVisible();
    });
  });

  it('shows error bubble on 503 busy', async () => {
    assistantServer.use(mockHealthEnabled, mockModels, mockChatBusy);
    render(<WrappedAssistantComponent />);
    
    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });

    const input = screen.getByPlaceholderText('Ask Recon AI about your cluster...');
    await userEvent.type(input, 'Hello');
    
    const sendButton = screen.getByRole('button', { name: /send/i });
    await userEvent.click(sendButton);

    await waitFor(() => {
      expect(screen.getByText('The chatbot is currently handling too many requests. Please try again in a moment.')).toBeVisible();
    });
  });

  it('shows error bubble on 504 timeout', async () => {
    assistantServer.use(mockHealthEnabled, mockModels, mockChatTimeout);
    render(<WrappedAssistantComponent />);
    
    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });

    const input = screen.getByPlaceholderText('Ask Recon AI about your cluster...');
    await userEvent.type(input, 'Hello');
    
    const sendButton = screen.getByRole('button', { name: /send/i });
    await userEvent.click(sendButton);

    await waitFor(() => {
      expect(screen.getByText('The chatbot request timed out. The LLM or Recon API took too long to respond. Please try again or use a different model.')).toBeVisible();
    });
  });

  it('shows error bubble on 500 error', async () => {
    assistantServer.use(mockHealthEnabled, mockModels, mockChatError);
    render(<WrappedAssistantComponent />);
    
    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });

    const input = screen.getByPlaceholderText('Ask Recon AI about your cluster...');
    await userEvent.type(input, 'Hello');
    
    const sendButton = screen.getByRole('button', { name: /send/i });
    await userEvent.click(sendButton);

    await waitFor(() => {
      expect(screen.getByText(/An error occurred while processing your request/)).toBeVisible();
      expect(screen.getByText('For more details, check the Recon server logs.')).toBeVisible();
    });
  });

  it('shows provider-specific error bubble on 500 error for OpenAI', async () => {
    assistantServer.use(mockHealthEnabled, mockModels, mockChatError);
    render(<WrappedAssistantComponent />);

    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });

    await userEvent.click(screen.getByText('Default Provider'));
    await userEvent.click(screen.getByText('OpenAI'));

    const input = screen.getByPlaceholderText('Ask Recon AI about your cluster...');
    await userEvent.type(input, 'Hello');
    await userEvent.click(screen.getByRole('button', { name: /send/i }));

    await waitFor(() => {
      expect(screen.getByText(/OpenAI could not complete this request/)).toBeVisible();
      expect(screen.getByText('For more details, check the Recon server logs.')).toBeVisible();
    });
  });

  it('shows provider-specific error bubble on 500 error for Gemini', async () => {
    assistantServer.use(mockHealthEnabled, mockModels, mockChatError);
    render(<WrappedAssistantComponent />);

    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });

    await userEvent.click(screen.getByText('Default Provider'));
    await userEvent.click(screen.getByText('Google Gemini'));

    const input = screen.getByPlaceholderText('Ask Recon AI about your cluster...');
    await userEvent.type(input, 'Hello');
    await userEvent.click(screen.getByRole('button', { name: /send/i }));

    await waitFor(() => {
      expect(screen.getByText(/Google Gemini could not complete this request/)).toBeVisible();
      expect(screen.getByText('For more details, check the Recon server logs.')).toBeVisible();
    });
  });

  it('shows provider-specific error bubble on 500 error for Anthropic', async () => {
    assistantServer.use(mockHealthEnabled, mockModels, mockChatError);
    render(<WrappedAssistantComponent />);

    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });

    await userEvent.click(screen.getByText('Default Provider'));
    await userEvent.click(screen.getByText('Anthropic Claude'));

    const input = screen.getByPlaceholderText('Ask Recon AI about your cluster...');
    await userEvent.type(input, 'Hello');
    await userEvent.click(screen.getByRole('button', { name: /send/i }));

    await waitFor(() => {
      expect(screen.getByText(/Anthropic Claude could not complete this request/)).toBeVisible();
      expect(screen.getByText('For more details, check the Recon server logs.')).toBeVisible();
    });
  });

  it('shows error bubble on 503 chat disabled', async () => {
    assistantServer.use(mockHealthEnabled, mockModels, mockChatDisabled);
    render(<WrappedAssistantComponent />);
    
    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });

    const input = screen.getByPlaceholderText('Ask Recon AI about your cluster...');
    await userEvent.type(input, 'Hello');
    
    const sendButton = screen.getByRole('button', { name: /send/i });
    await userEvent.click(sendButton);

    await waitFor(() => {
      expect(screen.getByText('Chatbot service is not enabled')).toBeVisible();
    });
  });

  it('shows error bubble on 503 interrupted', async () => {
    assistantServer.use(mockHealthEnabled, mockModels, mockChatInterrupted);
    render(<WrappedAssistantComponent />);
    
    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });

    const input = screen.getByPlaceholderText('Ask Recon AI about your cluster...');
    await userEvent.type(input, 'Hello');
    
    const sendButton = screen.getByRole('button', { name: /send/i });
    await userEvent.click(sendButton);

    await waitFor(() => {
      expect(screen.getByText('Request was interrupted. Please try again.')).toBeVisible();
    });
  });

  it('handles models fetch failure gracefully', async () => {
    assistantServer.use(mockHealthEnabled, mockModelsError);
    render(<WrappedAssistantComponent />);
    
    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });
    
    // Default model should be selected or available even if fetch fails
    expect(screen.getByText('Default Provider')).toBeVisible();
  });

  it('disables send button while in-flight', async () => {
    // Delay the response to test in-flight state
    assistantServer.use(
      mockHealthEnabled,
      mockModels,
      mockChatDelayed
    );
    
    render(<WrappedAssistantComponent />);
    
    await waitFor(() => {
      expect(screen.getByText('Welcome to Recon AI')).toBeVisible();
    });

    const input = screen.getByPlaceholderText('Ask Recon AI about your cluster...');
    await userEvent.type(input, 'Hello');
    
    const sendButton = screen.getByRole('button', { name: /send/i });
    await userEvent.click(sendButton);

    // Stop button should appear, send button should be gone or disabled
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /stop/i })).toBeVisible();
    });
    
    // Wait for completion
    await waitFor(() => {
      expect(screen.getByText('Delayed')).toBeVisible();
    });
  });
});
