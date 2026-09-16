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
import { test, expect } from '@playwright/test';

test.describe('Recon AI Error Handling Scenarios', () => {
  
  test('health-disabled', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({
        status: 200,
        json: { enabled: false, llmClientAvailable: true }
      });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Recon AI is Disabled')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/health-disabled.png' });
  });

  test('health-not-configured', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({
        status: 200,
        json: { enabled: true, llmClientAvailable: false }
      });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Recon AI is Not Configured')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/health-not-configured.png' });
  });

  test('empty-state', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({ status: 200, json: { enabled: true, llmClientAvailable: true } });
    });
    await page.route('**/api/v1/chatbot/models', async route => {
      await route.fulfill({ status: 200, json: { models: ['gemini-2.5-flash'] } });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Welcome to Recon AI')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/empty-state.png' });
  });

  test('chat-loading', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({ status: 200, json: { enabled: true, llmClientAvailable: true } });
    });
    await page.route('**/api/v1/chatbot/models', async route => {
      await route.fulfill({ status: 200, json: { models: ['gemini-2.5-flash'] } });
    });
    await page.route('**/api/v1/chatbot/chat', async route => {
      // Delay the response to capture the loading state
      await new Promise(resolve => setTimeout(resolve, 1000));
      await route.fulfill({ status: 200, json: { response: 'Done', success: true } });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Welcome to Recon AI')).toBeVisible();
    
    await page.fill('textarea', 'Hello');
    await page.keyboard.press('Enter');
    
    await expect(page.locator('.loading-bubble')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/chat-loading.png' });
  });

  test('chat-success', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({ status: 200, json: { enabled: true, llmClientAvailable: true } });
    });
    await page.route('**/api/v1/chatbot/models', async route => {
      await route.fulfill({ status: 200, json: { models: ['gemini-2.5-flash'] } });
    });
    await page.route('**/api/v1/chatbot/chat', async route => {
      await route.fulfill({ 
        status: 200, 
        json: { 
          response: 'This is a **Markdown** response with a table:\n\n| Col 1 | Col 2 |\n|---|---|\n| A | B |', 
          success: true 
        } 
      });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Welcome to Recon AI')).toBeVisible();
    
    await page.fill('textarea', 'Show me a table');
    await page.keyboard.press('Enter');
    
    await expect(page.locator('table')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/chat-success.png' });
  });

  test('chat-503-busy', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({ status: 200, json: { enabled: true, llmClientAvailable: true } });
    });
    await page.route('**/api/v1/chatbot/models', async route => {
      await route.fulfill({ status: 200, json: { models: ['gemini-2.5-flash'] } });
    });
    await page.route('**/api/v1/chatbot/chat', async route => {
      await route.fulfill({ 
        status: 503, 
        json: { error: 'The chatbot is currently handling too many requests. Please try again in a moment.' } 
      });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Welcome to Recon AI')).toBeVisible();
    
    await page.fill('textarea', 'Hello');
    await page.keyboard.press('Enter');
    
    await expect(page.locator('text=The chatbot is currently handling too many requests. Please try again in a moment.')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/chat-503-busy.png' });
  });

  test('chat-504-timeout', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({ status: 200, json: { enabled: true, llmClientAvailable: true } });
    });
    await page.route('**/api/v1/chatbot/models', async route => {
      await route.fulfill({ status: 200, json: { models: ['gemini-2.5-flash'] } });
    });
    await page.route('**/api/v1/chatbot/chat', async route => {
      await route.fulfill({ 
        status: 504, 
        json: { error: 'The chatbot request timed out. The LLM or Recon API took too long to respond. Please try again or use a different model.' } 
      });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Welcome to Recon AI')).toBeVisible();
    
    await page.fill('textarea', 'Hello');
    await page.keyboard.press('Enter');
    
    await expect(page.locator('text=The chatbot request timed out. The LLM or Recon API took too long to respond. Please try again or use a different model.')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/chat-504-timeout.png' });
  });

  test('chat-500-internal', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({ status: 200, json: { enabled: true, llmClientAvailable: true } });
    });
    await page.route('**/api/v1/chatbot/models', async route => {
      await route.fulfill({ status: 200, json: { models: ['gemini-2.5-flash'] } });
    });
    await page.route('**/api/v1/chatbot/chat', async route => {
      await route.fulfill({ 
        status: 500, 
        json: { error: 'An error occurred processing your request.' } 
      });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Welcome to Recon AI')).toBeVisible();
    
    await page.fill('textarea', 'Hello');
    await page.keyboard.press('Enter');
    
    await expect(page.locator('text=An error occurred while processing your request')).toBeVisible();
    await expect(page.locator('text=For more details, check the Recon server logs.')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/chat-500-internal.png' });
  });

  test('chat-503-interrupted', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({ status: 200, json: { enabled: true, llmClientAvailable: true } });
    });
    await page.route('**/api/v1/chatbot/models', async route => {
      await route.fulfill({ status: 200, json: { models: ['gemini-2.5-flash'] } });
    });
    await page.route('**/api/v1/chatbot/chat', async route => {
      await route.fulfill({ 
        status: 503, 
        json: { error: 'Request was interrupted. Please try again.' } 
      });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Welcome to Recon AI')).toBeVisible();
    
    await page.fill('textarea', 'Hello');
    await page.keyboard.press('Enter');
    
    await expect(page.locator('text=Request was interrupted. Please try again.')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/chat-503-interrupted.png' });
  });

  test('chat-503-disabled', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({ status: 200, json: { enabled: true, llmClientAvailable: true } });
    });
    await page.route('**/api/v1/chatbot/models', async route => {
      await route.fulfill({ status: 200, json: { models: ['gemini-2.5-flash'] } });
    });
    await page.route('**/api/v1/chatbot/chat', async route => {
      await route.fulfill({ 
        status: 503, 
        json: { error: 'Chatbot service is not enabled' } 
      });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Welcome to Recon AI')).toBeVisible();
    
    await page.fill('textarea', 'Hello');
    await page.keyboard.press('Enter');
    
    await expect(page.locator('text=Chatbot service is not enabled')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/chat-503-disabled.png' });
  });

  test('models-500', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({ status: 200, json: { enabled: true, llmClientAvailable: true } });
    });
    await page.route('**/api/v1/chatbot/models', async route => {
      await route.fulfill({ 
        status: 500, 
        json: { error: 'Failed to fetch models' } 
      });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Welcome to Recon AI')).toBeVisible();
    
    // Check that we can still use the chat with default provider
    await expect(page.locator('text=Default Provider')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/models-500.png' });
  });

  test('models-503', async ({ page }) => {
    await page.route('**/api/v1/chatbot/health', async route => {
      await route.fulfill({ status: 200, json: { enabled: true, llmClientAvailable: true } });
    });
    await page.route('**/api/v1/chatbot/models', async route => {
      await route.fulfill({ 
        status: 503, 
        json: { error: 'Chatbot service is not enabled' } 
      });
    });
    
    await page.goto('/#/Assistant');
    await expect(page.locator('text=Welcome to Recon AI')).toBeVisible();
    
    // Check that we can still use the chat with default provider
    await expect(page.locator('text=Default Provider')).toBeVisible();
    await page.screenshot({ path: 'e2e/screenshots/models-503.png' });
  });
});
