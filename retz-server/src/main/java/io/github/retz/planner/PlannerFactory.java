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
package io.github.retz.planner;

import io.github.retz.scheduler.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PlannerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PlannerFactory.class);

    public static Planner create(String name, ServerConfiguration serverConfig) throws Throwable {
        Properties properties = serverConfig.copyAsProperties();

        if (serverConfig.isBuiltInPlanner()) {

            if ("priority".equals(name)) {
                LOG.info("Using PriorityPlanner({})", name);
                return new PriorityPlanner();

            } else if ("fifo".equals(name)) {
                LOG.info("Using FIFOPlanner 2", name);
                String classname = "io.github.retz.planner.builtin.FIFOPlanner";
                return new ExtensiblePlanner(ExtensiblePlannerFactory.create(classname, serverConfig.classpath()), properties);

            } else if ("priority2".equals(name)) {
                LOG.info("Using PriorityPlanner 2", name);
                String classname = "io.github.retz.planner.builtin.PriorityPlanner";
                return new ExtensiblePlanner(ExtensiblePlannerFactory.create(classname, serverConfig.classpath()), properties);
            }
            LOG.info("Using FIFOPlanner");
            return new NaivePlanner();
        }

        return new ExtensiblePlanner(ExtensiblePlannerFactory.create(name, serverConfig.classpath()), properties);
    }
}
