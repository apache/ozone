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
package org.apache.hadoop.ozone.recon.chatbot.security;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CredentialHelper}.
 */
public class TestCredentialHelper {

    private static final String TEST_KEY = "ozone.recon.chatbot.test.api.key";
    private static final String TEST_SECRET = "sk-test-secret-12345";

    @TempDir
    private Path tempDir;

    private OzoneConfiguration conf;

    @BeforeEach
    public void setUp() {
        conf = new OzoneConfiguration();
    }

    @Test
    public void testReadFromJceks() throws IOException {
        // Create a JCEKS file with a test secret.
        String jceksPath = "jceks://file" +
                tempDir.resolve("test-credentials.jceks").toAbsolutePath();
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceksPath);

        // Populate the JCEKS store.
        CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);
        provider.createCredentialEntry(TEST_KEY, TEST_SECRET.toCharArray());
        provider.flush();

        // CredentialHelper should resolve from JCEKS.
        CredentialHelper helper = new CredentialHelper(conf);
        assertEquals(TEST_SECRET, helper.getSecret(TEST_KEY));
        assertTrue(helper.hasSecret(TEST_KEY));
    }

    @Test
    public void testFallbackToPlaintext() {
        // No JCEKS configured — set the key as plaintext in config.
        conf.set(TEST_KEY, TEST_SECRET);

        CredentialHelper helper = new CredentialHelper(conf);
        assertEquals(TEST_SECRET, helper.getSecret(TEST_KEY));
        assertTrue(helper.hasSecret(TEST_KEY));
    }

    @Test
    public void testMissingKeyReturnsEmpty() {
        // No JCEKS, no plaintext — should return empty string.
        CredentialHelper helper = new CredentialHelper(conf);
        assertEquals("", helper.getSecret(TEST_KEY));
        assertFalse(helper.hasSecret(TEST_KEY));
    }

    @Test
    public void testJceksTakesPriorityOverPlaintext() throws IOException {
        String jceksSecret = "jceks-secret";
        String plaintextSecret = "plaintext-secret";

        // Set up both JCEKS and plaintext.
        String jceksPath = "jceks://file" +
                tempDir.resolve("priority-test.jceks").toAbsolutePath();
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceksPath);
        conf.set(TEST_KEY, plaintextSecret);

        CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);
        provider.createCredentialEntry(TEST_KEY, jceksSecret.toCharArray());
        provider.flush();

        // JCEKS value should win.
        CredentialHelper helper = new CredentialHelper(conf);
        assertEquals(jceksSecret, helper.getSecret(TEST_KEY));
    }

    @Test
    public void testMultipleKeysInSameJceks() throws IOException {
        String key1 = "ozone.recon.chatbot.openai.api.key";
        String key2 = "ozone.recon.chatbot.gemini.api.key";
        String secret1 = "sk-openai-key";
        String secret2 = "AIza-gemini-key";

        String jceksPath = "jceks://file" +
                tempDir.resolve("multi-key-test.jceks").toAbsolutePath();
        conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jceksPath);

        CredentialProvider provider = CredentialProviderFactory.getProviders(conf).get(0);
        provider.createCredentialEntry(key1, secret1.toCharArray());
        provider.createCredentialEntry(key2, secret2.toCharArray());
        provider.flush();

        CredentialHelper helper = new CredentialHelper(conf);
        assertEquals(secret1, helper.getSecret(key1));
        assertEquals(secret2, helper.getSecret(key2));
        assertTrue(helper.hasSecret(key1));
        assertTrue(helper.hasSecret(key2));
    }
}
