/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.github.retz.cli;

import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class LauncherTest {
    @Test
    public void parseLoadAppTest() throws IOException, URISyntaxException {
        {
            String[] argv = {"-C", "src/test/resources/retz.properties", "load-app", "-A", "t"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertNotNull(conf);
            assertEquals("load-app", conf.commander.getParsedCommand());
        }

        {
            String[] argv = {"-C", "src/test/resources/retz.properties", "load-app", "-A", "t", "-F", "file://foo/bar/baz"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("load-app", conf.commander.getParsedCommand());
            assertEquals("load-app", conf.getParsedSubCommand().getName());
            assertTrue(conf.getParsedSubCommand() instanceof CommandLoadApp);
            CommandLoadApp command = (CommandLoadApp) conf.getParsedSubCommand();
            assertFalse(command.files.isEmpty());
            assertEquals("file://foo/bar/baz", command.files.get(0));
        }

        {
            String[] argv = {"-C", "src/test/resources/retz.properties", "load-app", "-A", "t",
                    "-F", "file://foo/bar/baz", "-F", "http://example.com/example.tar.gz"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("load-app", conf.commander.getParsedCommand());
            assertEquals("load-app", conf.getParsedSubCommand().getName());
            assertTrue(conf.getParsedSubCommand() instanceof CommandLoadApp);
            CommandLoadApp command = (CommandLoadApp) conf.getParsedSubCommand();
            assertFalse(command.files.isEmpty());
            assertEquals("file://foo/bar/baz", command.files.get(0));
            assertEquals("http://example.com/example.tar.gz", command.files.get(1));
        }
        // TODO: add more pattern tests
    }

    @Test
    public void parseRunTest() throws IOException, URISyntaxException {
        {
            String[] argv = {"-C", "src/test/resources/retz.properties", "run", "-A", "t",
                    "-cmd", "uname -a", "-E", "a=b", "-cpu", "2"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("run", conf.commander.getParsedCommand());
            CommandRun command = (CommandRun) conf.getParsedSubCommand();
            assertEquals("a=b", command.envs.get(0));
        }
    }

    @Test
    public void pairParserTest() {
        String[] envs = {"k=v", "a=b", "FOOBAR='LOOK_AT ME DONT'", "wat", "empty=", "=not"};
        Properties props = SubCommand.parseKeyValuePairs(Arrays.asList(envs));
        for(Map.Entry<Object, Object> e : props.entrySet()) {
            System.err.println(e.getKey() + " = " + e.getValue());
        }
        assertEquals(4, props.size());
        assertEquals("v", props.getProperty("k"));
        assertEquals("b", props.getProperty("a"));
        assertEquals("'LOOK_AT ME DONT'", props.getProperty("FOOBAR"));
        assertEquals(null, props.getProperty("wat"));
        assertEquals("", props.getProperty("empty"));
        assertEquals(null, props.getProperty(""));
        assertEquals(null, props.getProperty("not"));
        assertEquals(null, props.getProperty("other foobar"));
    }

    @Test
    public void pairParserTest2() {
        String[] envs = {"ASAKUSA_M3BP_ARGS=\"--engine-conf com.asakusafw.m3bp.output.buffer.size=209 --engine-conf com.asakusafw.m3bp.output.buffer.flush=0.6 --engine-conf com.asakusafw.m3bp.thread.max=\\$RETZ_CPU\""};
        Properties props = SubCommand.parseKeyValuePairs(Arrays.asList(envs));
        assertEquals(1, props.size());
        assertEquals(
                "\"--engine-conf com.asakusafw.m3bp.output.buffer.size=209 --engine-conf com.asakusafw.m3bp.output.buffer.flush=0.6 --engine-conf com.asakusafw.m3bp.thread.max=\\$RETZ_CPU\"",
                props.getProperty("ASAKUSA_M3BP_ARGS"));

    }
}
