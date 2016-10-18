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

import com.j256.simplejmx.client.JmxClient;
import io.github.retz.cli.FileConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.ObjectName;

public class CommandListUser implements SubCommand {
    static final Logger LOG = LoggerFactory.getLogger(CommandListUser.class);

    @Override
    public String description() {
        return "List all users";
    }

    @Override
    public String getName() {
        return "list-user";
    }

    @Override
    public int handle(FileConfiguration fileConfig) {
        try(JmxClient jmxClient = new JmxClient("localhost", 9999)) {
        /*
        Set<ObjectName> objectNames = jmxClient.getBeanNames();
        for (ObjectName on : objectNames) {
            LOG.info(on.toString());
        }
        jmxClient.invokeOperation(new ObjectName("java.lang:type=Memory"), "gc");
        */
            Object o = jmxClient.invokeOperation(new ObjectName("io.github.retz.scheduler:type=AdminConsole"), "listUser");
            LOG.debug("{}: {}", o.getClass().getName(), o.toString());
            String[] users = (String[]) o;
            //List<String> users = (List<String>)o;
            for (String user : users) {
                LOG.info(user);
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

