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
import com.beust.jcommander.MissingCommandException;
import com.beust.jcommander.ParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

public class Launcher {
    static final Logger LOG = LoggerFactory.getLogger(Launcher.class);

    private static final List<SubCommand> SUB_COMMANDS;

    static {
        SUB_COMMANDS = new LinkedList<>();

        SUB_COMMANDS.add(new CommandConfig());
        SUB_COMMANDS.add(new CommandList());
        SUB_COMMANDS.add(new CommandSchedule());
        SUB_COMMANDS.add(new CommandGetJob());
        SUB_COMMANDS.add(new CommandRun());
        SUB_COMMANDS.add(new CommandWatch());
        SUB_COMMANDS.add(new CommandListApp());
        SUB_COMMANDS.add(new CommandLoadApp());
        SUB_COMMANDS.add(new CommandUnloadApp());
    }

    public static void main(String... argv) {
        int status = execute(argv);
        System.exit(status);
    }

    public static int execute(String... argv) {
        try {
            Configuration conf = parseConfiguration(argv);
            JCommander commander = conf.commander;

            if (commander.getParsedCommand() != null) {
                LOG.info("Command: {}, Config file: {}", commander.getParsedCommand(),
                        conf.commands.getConfigFile());
                LOG.debug("Configuration: {}", conf.fileConfiguration.toString());
                return conf.getParsedSubCommand().handle(conf.fileConfiguration);

            } else {
                LOG.error("Invalid subcommand");
                help(SUB_COMMANDS);
            }

        } catch (IOException e) {
            LOG.error("Invalid configuration file: {}", e.toString());
        } catch (URISyntaxException e) {
            LOG.error("Bad file format: {}", e.toString());
        } catch (MissingCommandException e) {
            LOG.error(e.toString());
            help(SUB_COMMANDS);
        } catch (ParameterException e) {
            LOG.error("{}", e.toString());
            help(SUB_COMMANDS);
        }
        //$ retz kill <jobid>
        //$ retz schedule -file <list of batches in a text file>
        return -1;
    }

    private static boolean oneOf(String key, String... list) {
        for (String s : list) {
            if (key.equals(s)) return true;
        }
        return false;
    }

    private static void help(List<SubCommand> subCommands) {
        LOG.info("Subcommands:");
        for (SubCommand subCommand : subCommands) {
            LOG.info("\t{}\t{} ({})", subCommand.getName(),
                    subCommand.description(), subCommand.getClass().getName());
        }
    }

    static Configuration parseConfiguration(String... argv) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();

        conf.commands = new MainCommand();
        conf.commander = new JCommander(conf.commands);

        for (SubCommand subCommand : SUB_COMMANDS) {
            subCommand.add(conf.commander);
        }

        conf.commander.parse(argv);

        conf.fileConfiguration = new FileConfiguration(conf.commands.getConfigFile());
        return conf;
    }

    static class Configuration {
        FileConfiguration fileConfiguration;
        JCommander commander;
        MainCommand commands;

        SubCommand getParsedSubCommand() {
            for (SubCommand subCommand : SUB_COMMANDS) {
                if (subCommand.getName().equals(commander.getParsedCommand())) {
                    return subCommand;
                }
            }
            throw new ParameterException("unknown command: " + commander.getParsedCommand());
        }
    }
}

