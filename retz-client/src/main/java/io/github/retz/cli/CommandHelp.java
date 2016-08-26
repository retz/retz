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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.github.retz.cli.Launcher.help;

public class CommandHelp implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandHelp.class);

    JCommander commander;

    public void add(JCommander commander) {
        this.commander = commander;
        commander.addCommand(getName(), this, description());
    }


    @Parameter(names = {"-s", "--subcommand"}, description = "Subcommand to see help")
    private String command;

    @Override
    public String description() {
        return "Print help ('-s <subcommand>' see detail options)";
    }

    @Override
    public String getName() {
        return "help";
    }

    @Override
    public int handle(FileConfiguration fileConfig) {
        if (command == null) {
            help();
        } else {
            StringBuilder builder = new StringBuilder();
            this.commander.usage(command, builder);
            LOG.info(builder.toString());
        }
        return 0;
    }
}
