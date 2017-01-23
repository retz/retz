/**
 *    Retz
 *    Copyright (C) 2016-2017 Nautilus Technologies, Inc.
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

import io.github.retz.protocol.data.DockerVolume;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class LauncherTest {
    static final String PROPERTY_FILE = "src/test/resources/retz.properties";

    @Test
    public void parseLoadAppTest() throws IOException, URISyntaxException {
        {
            String[] argv = {"-C", PROPERTY_FILE, "load-app", "-A", "t"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertNotNull(conf);
            assertEquals("load-app", conf.commander.getParsedCommand());
        }

        {
            String[] argv = {"-C", PROPERTY_FILE, "load-app", "-A", "t", "-F", "file://foo/bar/baz",
            "--enabled", "false"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("load-app", conf.commander.getParsedCommand());
            assertEquals("load-app", conf.getParsedSubCommand().getName());
            assertTrue(conf.getParsedSubCommand() instanceof CommandLoadApp);
            CommandLoadApp command = (CommandLoadApp) conf.getParsedSubCommand();
            assertFalse(command.files.isEmpty());
            assertEquals("file://foo/bar/baz", command.files.get(0));
            assertEquals("mesos", command.container);
            assertEquals("false", command.enabledStr);
        }

        {
            String[] argv = {"-C", PROPERTY_FILE, "load-app", "-A", "t",
                    "-F", "file://foo/bar/baz", "-F", "http://example.com/example.tar.gz",
                    "-L", "file://large", "--large-file", "file://large2"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("load-app", conf.commander.getParsedCommand());
            assertEquals("load-app", conf.getParsedSubCommand().getName());
            assertTrue(conf.getParsedSubCommand() instanceof CommandLoadApp);
            CommandLoadApp command = (CommandLoadApp) conf.getParsedSubCommand();
            assertFalse(command.files.isEmpty());
            assertEquals("file://foo/bar/baz", command.files.get(0));
            assertEquals("http://example.com/example.tar.gz", command.files.get(1));
            assertEquals("file://large", command.largeFiles.get(0));
            assertEquals("file://large2", command.largeFiles.get(1));
            assertEquals("mesos", command.container);
        }

        {
            String[] argv = {"-C", PROPERTY_FILE, "load-app", "-A", "t",
                    "--container", "docker", "--image", "ubuntu:latest"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("load-app", conf.commander.getParsedCommand());
            assertEquals("load-app", conf.getParsedSubCommand().getName());
            assertTrue(conf.getParsedSubCommand() instanceof CommandLoadApp);
            CommandLoadApp command = (CommandLoadApp) conf.getParsedSubCommand();
            assertEquals("docker", command.container);
            assertEquals("ubuntu:latest", command.image);
        }

        {
            String[] argv = {"-C", PROPERTY_FILE, "load-app", "-A", "t",
                    "--container", "docker", "--image", "ubuntu:latest",
                    "--docker-volumes", "d:n:cp,d2:n2:cp2:mode=RW:key=value-boo",
                    "--docker-volumes", "nfs:192.168.0.1/:my-nfs:mode=RW:user=u:pass=p"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("load-app", conf.commander.getParsedCommand());
            assertEquals("load-app", conf.getParsedSubCommand().getName());
            assertTrue(conf.getParsedSubCommand() instanceof CommandLoadApp);
            CommandLoadApp command = (CommandLoadApp) conf.getParsedSubCommand();
            assertEquals("docker", command.container);
            assertEquals("ubuntu:latest", command.image);

            List<DockerVolume> volumes = CommandLoadApp.parseDockerVolumeSpecs(command.volumeSpecs);
            assertEquals(3, volumes.size());
            {
                DockerVolume v = volumes.get(0);
                assertEquals("d", v.driver());
                assertEquals("n", v.name());
                assertEquals("cp", v.containerPath());
                assertEquals(DockerVolume.Mode.RO, v.mode());
                assertEquals(0, v.options().size());
            }
            {
                DockerVolume v = volumes.get(1);
                assertEquals("d2", v.driver());
                assertEquals("cp2", v.containerPath());
                assertEquals(DockerVolume.Mode.RW, v.mode());
                assertEquals("value-boo", v.options().getProperty("key"));
            }
            {
                DockerVolume v = volumes.get(2);
                assertEquals("nfs", v.driver());
                assertEquals("192.168.0.1/", v.name());
                assertEquals("my-nfs", v.containerPath());
                assertEquals(DockerVolume.Mode.RW, v.mode());
                assertEquals("u", v.options().getProperty("user"));
                assertEquals("p", v.options().getProperty("pass"));
            }
        }
    }

    @Test
    public void parseScheduleTest() throws IOException, URISyntaxException {
        {
            String[] argv = {"-C", PROPERTY_FILE, "schedule", "-A", "t", "-cmd", "uname -a"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("schedule", conf.getParsedSubCommand().getName());
        }

        {
            String[] argv = {"-C", PROPERTY_FILE, "schedule", "-A", "t", "-cmd", "uname -a", "-N", "fooname", "--prio", "-2"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("schedule", conf.getParsedSubCommand().getName());
            CommandSchedule commandSchedule = (CommandSchedule)conf.getParsedSubCommand();
            assertEquals("fooname", commandSchedule.name);
            assertEquals(-2, commandSchedule.priority);
        }

        {
            String[] argv = {"-C", PROPERTY_FILE, "schedule", "-A", "t", "-cmd", "uname -a", "--tags", "a,b,c,234"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("schedule", conf.getParsedSubCommand().getName());
            CommandSchedule commandSchedule = (CommandSchedule)conf.getParsedSubCommand();
            assertEquals(4, commandSchedule.tags.size());
            assertEquals("a", commandSchedule.tags.get(0));
            assertEquals("b", commandSchedule.tags.get(1));
            assertEquals("c", commandSchedule.tags.get(2));
            assertEquals("234", commandSchedule.tags.get(3));
        }

        {
            String[] argv = {"-C", PROPERTY_FILE, "schedule", "-A", "t", "-cmd", "uname -a", "-E", "a=b", "-E", "c=d",
                    "-E", "CUDA_PATH=/usr/local/cuda"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("schedule", conf.getParsedSubCommand().getName());
            CommandSchedule commandSchedule = (CommandSchedule)conf.getParsedSubCommand();
            Properties p = SubCommand.parseKeyValuePairs(commandSchedule.envs);
            assertEquals("b", p.getProperty("a"));
            assertEquals("d", p.getProperty("c"));
            assertEquals("/usr/local/cuda", p.getProperty("CUDA_PATH"));
        }
        // TODO: add more pattern tests
    }

    @Test
    public void parseRunTest() throws IOException, URISyntaxException {
        {
            String[] argv = {"-C", PROPERTY_FILE, "run", "-A", "t",
                    "-cmd", "uname -a", "-E", "a=b", "-cpu", "2", "--disk", "512"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("run", conf.commander.getParsedCommand());
            CommandRun command = (CommandRun) conf.getParsedSubCommand();
            assertEquals("a=b", command.envs.get(0));
            assertEquals(2, command.cpu);
            assertEquals(32, command.mem);
            assertEquals(0, command.gpu);
            assertEquals(512, command.disk);
            assertFalse(command.stderr);
        }

        {
            String[] argv = {"-C", PROPERTY_FILE, "run", "-A", "t",
                    "-cmd", "uname -a", "-E", "a=b", "-cpu", "2", "-gpu", "1", "-stderr"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("run", conf.commander.getParsedCommand());
            CommandRun command = (CommandRun) conf.getParsedSubCommand();
            assertEquals("a=b", command.envs.get(0));
            assertEquals(2, command.cpu);
            assertEquals(32, command.mem);
            assertEquals(1, command.gpu);
            assertTrue(command.stderr);
        }

        {
            String[] argv = {"-C", PROPERTY_FILE, "run", "-A", "t", "-cmd", "uname -a", "-N", "fooname", "--prio", "-2"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("run", conf.getParsedSubCommand().getName());
            CommandRun commandRun = (CommandRun)conf.getParsedSubCommand();
            assertEquals("fooname", commandRun.name);
            assertEquals(-2, commandRun.priority);
        }
    }

    @Test
    public void pairParserTest() {
        String[] envs = {"k=v", "a=b", "FOOBAR='LOOK_AT ME DONT'", "wat", "empty=", "=not"};
        Properties props = SubCommand.parseKeyValuePairs(Arrays.asList(envs));
        for (Map.Entry<Object, Object> e : props.entrySet()) {
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
