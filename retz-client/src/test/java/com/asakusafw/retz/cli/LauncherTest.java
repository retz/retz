/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, KK.
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
package com.asakusafw.retz.cli;

import org.apache.commons.cli.ParseException;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

public class LauncherTest {
    @Test
    public void parseTest () throws ParseException, IOException, URISyntaxException {
        String[] argv = {"load-app", "-C", "src/test/resources/retz.properties"};
        // System.err.println(System.getProperty("user.dir"));
        Launcher.Configuration conf = Launcher.parseConfiguration(argv);
        assert conf != null;
        // TODO: add more pattern tests
    }
}
