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

import com.beust.jcommander.JCommander;
import io.github.retz.cli.FileConfiguration;

import java.util.List;
import java.util.Properties;

public interface SubCommand {

    default void add(JCommander commander) {
        commander.addCommand(getName(), this, description());
    }

    String description();

    int handle(FileConfiguration fileConfig);

    String getName();

    static Properties parseKeyValuePairs(List<String> pairs) {
        Properties props = new Properties();
        if (pairs == null) {
            return props;
        }

        for (String e : pairs) {
            int p = e.indexOf('=');
            if (p > 0) {
                props.put(e.substring(0, p), e.substring(p + 1));
            }
        }
        return props;
    }
}
