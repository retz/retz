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
package io.github.retz.jmx;

import com.j256.simplejmx.server.JmxServer;
import io.github.retz.db.Database;
import io.github.retz.misc.LogUtil;
import io.github.retz.scheduler.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class RetzJmxServer {
    private static final Logger LOG = LoggerFactory.getLogger(RetzJmxServer.class);

    // Singleton
    private static JmxServer jmxServer = null;

    private static void registerMBean(Object mbean, String name) {
        try {
            ManagementFactory.getPlatformMBeanServer().registerMBean(mbean, new ObjectName(name));
        } catch (JMException e) {
            LogUtil.error(LOG, "RetzJmxServer.registerMBean() failed with " + name, e);
        }
    }

    public static synchronized void start(ServerConfiguration config) throws JMException {
        if (jmxServer != null) {
            throw new RuntimeException("Tried to start duplicate JMX server");
        }
        int jmxPort = config.getJmxPort();

        registerMBean(new AdminConsole(config.getGcLeeway()), "io.github.retz.scheduler:type=AdminConsole");
        registerMBean(Database.getDataSource().getPool().getJmxPool(), "io.github.retz.db:type=TomcatThreadPool");
        registerMBean(new StatusAdapter(), "io.github.retz:type=Stats,name=Status");
        registerMBean(ResourceQuantityAdapter.newTotalOfferedQuantityAdapter() , "io.github.retz:type=Stats,name=TotalOffered");
        registerMBean(ResourceQuantityAdapter.newTotalUsedQuantityAdapter(), "io.github.retz:type=Stats,name=TotalUsed");

        jmxServer = new JmxServer(jmxPort);
        jmxServer.start();
        LOG.info("JMX enabled listening to {}", jmxPort);
    }

    public static synchronized void stop() {
        if (jmxServer == null) {
            return;
        }

        jmxServer.stop();
        jmxServer = null;
    }
}
