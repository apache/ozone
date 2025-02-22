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

package org.apache.hadoop.ozone.shell;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Supplier;
import org.jline.console.SystemRegistry;
import org.jline.console.impl.SystemRegistryImpl;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.Parser;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.widget.TailTipWidgets;
import org.jline.widget.TailTipWidgets.TipType;
import picocli.CommandLine;
import picocli.shell.jline3.PicocliCommands;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

/**
 * Interactive shell for Ozone commands.
 * (REPL = Read-Eval-Print Loop)
 */
class REPL {

  REPL(Shell shell, CommandLine cmd, PicocliCommandsFactory factory, List<String> lines) {
    Parser parser = new DefaultParser();
    Supplier<Path> workDir = () -> Paths.get(System.getProperty("user.dir"));
    TerminalBuilder terminalBuilder = TerminalBuilder.builder()
        .dumb(true);
    try (Terminal terminal = terminalBuilder.build()) {
      factory.setTerminal(terminal);

      PicocliCommands picocliCommands = new PicocliCommands(cmd);
      picocliCommands.name(shell.name());
      SystemRegistry registry = new SystemRegistryImpl(parser, terminal, workDir, null);
      registry.setCommandRegistries(picocliCommands);
      registry.register("help", picocliCommands);

      LineReader reader = LineReaderBuilder.builder()
          .terminal(terminal)
          .completer(registry.completer())
          .parser(parser)
          .variable(LineReader.LIST_MAX, 50)
          .build();

      if (!Terminal.TYPE_DUMB.equals(terminal.getType()) && !Terminal.TYPE_DUMB_COLOR.equals(terminal.getType())) {
        TailTipWidgets widgets = new TailTipWidgets(reader, registry::commandDescription, 5, TipType.COMPLETER);
        widgets.enable();
      }

      String prompt = shell.prompt() + "> ";

      final int batchSize = lines == null ? 0 : lines.size();
      if (batchSize > 0) {
        terminal.echo(true);
        reader.addCommandsInBuffer(lines);
      }

      for (int i = 0; batchSize == 0 || i < batchSize; i++) {
        try {
          registry.cleanUp();
          String line = reader.readLine(prompt, null, (MaskingCallback) null, null);
          registry.execute(line);
        } catch (UserInterruptException ignored) {
          // ignore
        } catch (EndOfFileException e) {
          return;
        } catch (Exception e) {
          registry.trace(e);
        }
      }
    } catch (Exception e) {
      shell.printError(e);
    }
  }
}
