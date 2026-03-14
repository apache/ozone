/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.chatbot;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.apache.hadoop.ozone.recon.chatbot.agent.ChatbotAgent;
import org.apache.hadoop.ozone.recon.chatbot.agent.ToolExecutor;
import org.apache.hadoop.ozone.recon.chatbot.api.ChatbotEndpoint;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMProvider;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMProviderRouter;
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;

/**
 * Guice module for Chatbot dependency injection.
 */
public class ChatbotModule extends AbstractModule {

  @Override
  protected void configure() {
    // Bind credential helper (JCEKS key management)
    bind(CredentialHelper.class).in(Scopes.SINGLETON);

    // Bind LLM provider — router delegates to direct providers
    bind(LLMProvider.class).to(LLMProviderRouter.class).in(Scopes.SINGLETON);

    // Bind agent components
    bind(ToolExecutor.class).in(Scopes.SINGLETON);
    bind(ChatbotAgent.class).in(Scopes.SINGLETON);

    // Bind API endpoint
    bind(ChatbotEndpoint.class).in(Scopes.SINGLETON);
  }
}
