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

package org.apache.hadoop.ozone.upgrade;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableSet;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AbstractUpgradeActionProvider#load()}.
 */
class TestAbstractUpgradeActionProvider {

  /**
   * An upgrade action whose no-arg constructor fails to instantiate must abort {@code load()} rather than
   * silently drop the action from the returned map, which would finalize its version as a no-op.
   */
  @Test
  public void testLoadFailsWhenActionCannotBeInstantiated() {
    assertThrows(IllegalStateException.class, () -> new ThrowingActionProvider().load());
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  private @interface TestUpgradeAction {
  }

  private interface TestAction extends UpgradeAction<Object> {
  }

  @TestUpgradeAction
  public static class ThrowingTestAction implements TestAction {
    ThrowingTestAction() {
      throw new IllegalArgumentException("cannot construct test upgrade action");
    }

    @Override
    public void execute(Object arg) {
    }
  }

  private static final class ThrowingActionProvider extends AbstractUpgradeActionProvider<TestAction> {
    ThrowingActionProvider() {
      super(ImmutableSet.of(TestUpgradeAction.class), TestAction.class,
          TestAbstractUpgradeActionProvider.class.getPackage().getName());
    }

    @Override
    protected ComponentVersion extractVersion(Class<?> clazz) {
      return HDDSLayoutFeature.INITIAL_VERSION;
    }
  }
}
