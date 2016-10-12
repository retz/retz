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
import io.github.retz.cli.FileConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.ObjectName;

public class CommandUsage implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandUsage.class);

    @Parameter(names = "-id", description = "Get usage of a user", required = true)
    private int id;

    @Override
    public String description() {
        return "Get usage of a user";
    }

    @Override
    public String getName() {
        return "usage";
    }

    @Override
    public int handle(FileConfiguration fileConfig) {
        try(ClosableJmxClient jmxClient = new ClosableJmxClient("localhost", 9999)) {
            Object o = jmxClient.invokeOperation(new ObjectName("io.github.retz.scheduler:type=AdminConsole"), "createUser");
            String[] s = (String[])o;
            for (String line : s) {
                LOG.info(line);
            }
            return 0;
        } catch (JMException e){
            LOG.error(e.toString());
        } catch (Exception e) {
            LOG.error(e.toString());
        }
        return -1;
    }
}

