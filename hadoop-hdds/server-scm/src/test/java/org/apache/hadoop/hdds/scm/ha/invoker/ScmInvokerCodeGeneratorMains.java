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

package org.apache.hadoop.hdds.scm.ha.invoker;

import org.apache.hadoop.hdds.scm.block.DeletedBlockLogStateManager;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineStateManager;
import org.apache.hadoop.hdds.scm.security.RootCARotationHandler;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationStateManager;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyState;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;

/** Main methods for running {@link ScmInvokerCodeGenerator}. */
class ScmInvokerCodeGeneratorMains {

  static class GenerateDeletedBlockLogStateManager {
    public static void main(String... args) {
      ScmInvokerCodeGenerator.generate(DeletedBlockLogStateManager.class, true);
    }
  }

  static class GenerateContainerStateManager {
    public static void main(String... args) {
      ScmInvokerCodeGenerator.generate(ContainerStateManager.class, true);
    }
  }

  static class GeneratePipelineStateManager {
    public static void main(String... args) {
      ScmInvokerCodeGenerator.generate(PipelineStateManager.class, true);
    }
  }

  static class GenerateRootCARotationHandler {
    public static void main(String... args) {
      ScmInvokerCodeGenerator.generate(RootCARotationHandler.class, true);
    }
  }

  static class GenerateFinalizationStateManager {
    public static void main(String... args) {
      ScmInvokerCodeGenerator.generate(FinalizationStateManager.class, true);
    }
  }

  static class GenerateSecretKeyState {
    public static void main(String... args) {
      ScmInvokerCodeGenerator.generate(SecretKeyState.class, true);
    }
  }

  static class GenerateSequenceIdGeneratorStateManager {
    public static void main(String... args) {
      ScmInvokerCodeGenerator.generate(SequenceIdGenerator.StateManager.class, true);
    }
  }

  static class GenerateStatefulServiceStateManager {
    public static void main(String... args) {
      ScmInvokerCodeGenerator.generate(StatefulServiceStateManager.class, true);
    }
  }

  static class GenerateCertificateStore {
    public static void main(String... args) {
      ScmInvokerCodeGenerator.generate(CertificateStore.class, true);
    }
  }

  static class All {
    public static void main(String... args) {
      GenerateCertificateStore.main();
      GenerateContainerStateManager.main();
      GenerateDeletedBlockLogStateManager.main();
      GenerateFinalizationStateManager.main();
      GeneratePipelineStateManager.main();
      GenerateRootCARotationHandler.main();
      GenerateSecretKeyState.main();
      GenerateSequenceIdGeneratorStateManager.main();
      GenerateStatefulServiceStateManager.main();
    }
  }
}
