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

import com.beust.jcommander.Parameter;

;

public class MainCommand {
    @Parameter(names={"--config", "-C"})
    private String configFile = "/opt/retz-server/etc/retz.properties";

    @Parameter(names={"--verbose", "-v"})
    boolean verbose = false;

    public String getConfigFile() {
        // Allowing null for now; no configuration file may even work
        return configFile;
    }
}
