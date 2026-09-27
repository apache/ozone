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

package org.apache.hadoop.ozone.recon.chatbot;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.apache.hadoop.ozone.recon.chatbot.agent.ChatbotAgent;
import org.apache.hadoop.ozone.recon.chatbot.agent.LlmToolSpecFactory;
import org.apache.hadoop.ozone.recon.chatbot.api.ChatbotEndpoint;
import org.apache.hadoop.ozone.recon.chatbot.llm.LLMClient;
import org.apache.hadoop.ozone.recon.chatbot.llm.LangChain4jDispatcher;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconApiAllowlist;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconEndpointRouter;
import org.apache.hadoop.ozone.recon.chatbot.recon.ReconQueryExecutor;
import org.apache.hadoop.ozone.recon.chatbot.security.CredentialHelper;

/**
 * Guice module for Chatbot dependency injection.
 */
public class ChatbotModule extends AbstractModule {

  @Override
  protected void configure() {
    // Bind credential helper (JCEKS key management)
    bind(CredentialHelper.class).in(Scopes.SINGLETON);

    // Bind LLM provider — LangChain4j-backed dispatcher handles all three providers
    bind(LLMClient.class).to(LangChain4jDispatcher.class).in(Scopes.SINGLETON);

    // Recon data access (direct endpoint bean calls)
    bind(ReconEndpointRouter.class).in(Scopes.SINGLETON);
    bind(ReconApiAllowlist.class).in(Scopes.SINGLETON);
    bind(ReconQueryExecutor.class).in(Scopes.SINGLETON);
    bind(LlmToolSpecFactory.class).in(Scopes.SINGLETON);
    bind(ChatbotAgent.class).in(Scopes.SINGLETON);

    // Bind API endpoint
    bind(ChatbotEndpoint.class).in(Scopes.SINGLETON);
  }
}
