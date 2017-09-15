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
package io.github.retz.misc;

import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Container;
import io.github.retz.protocol.data.MesosContainer;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ApplicationBuilder {
    private String appid;
    private List<String> largeFiles;
    private List<String> files;
    // User is just a String that specifies Unix user
    private Optional<String> user;
    // Owner is Retz access key
    private String owner;
    private int gracePeriod = 0; // if 0, no KillPolicy will be added to CommandInfo

    private Container container;
    private boolean enabled;

    public ApplicationBuilder(String name, String owner) {
        appid = name;
        this.owner = owner;
        largeFiles = Collections.emptyList();
        files = Collections.emptyList();
        user = Optional.empty();
        container = new MesosContainer();
        enabled = true;
    }

    public Application build() {
        return new Application(appid, largeFiles, files, user, owner, gracePeriod, container, enabled);
    }
}
