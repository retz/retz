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
package io.github.retz.admin;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LauncherTest {
    static final String CONFIGFILE = "src/test/resources/retz.properties";
    @Test
    public void cli() throws Exception {
        {
            String[] argv = {"-C", CONFIGFILE, "create-user"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("create-user", conf.getParsedSubCommand().getName());
        }
        {
            String[] argv = {"-C", CONFIGFILE, "create-user", "--info", "{it's a json}"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("create-user", conf.getParsedSubCommand().getName());
            assertEquals("{it's a json}", ((CommandCreateUser)conf.getParsedSubCommand()).info);
        }

        {
            String[] argv = {"-C", CONFIGFILE, "create-users", "--file", "deadbeef-file", "--output", "cafebab3e-output"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("create-users", conf.getParsedSubCommand().getName());
            assertEquals("deadbeef-file", ((CommandCreateUsers)conf.getParsedSubCommand()).file);
            assertEquals("cafebab3e-output", ((CommandCreateUsers)conf.getParsedSubCommand()).dir);
        }

        {
            String[] argv = {"-C", CONFIGFILE, "disable-user", "-id", "kao"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("disable-user", conf.getParsedSubCommand().getName());
        }
        {
            String[] argv = {"-C", CONFIGFILE, "enable-user", "-id", "kao"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("enable-user", conf.getParsedSubCommand().getName());
        }
        {
            String[] argv = {"-C", CONFIGFILE, "get-user", "-id", "kao"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("get-user", conf.getParsedSubCommand().getName());
        }
        {
            String[] argv = {"-C", CONFIGFILE, "help", "-s", "get-user"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("help", conf.getParsedSubCommand().getName());
        }
        {
            String[] argv = {"-C", CONFIGFILE, "list-user"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("list-user", conf.getParsedSubCommand().getName());
        }
        {
            String[] argv = {"-C", CONFIGFILE, "usage", "-start", "2033"};
            Launcher.Configuration conf = Launcher.parseConfiguration(argv);
            assertEquals("usage", conf.getParsedSubCommand().getName());
        }
    }
}
