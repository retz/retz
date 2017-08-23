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
import io.github.retz.misc.LogUtil;
import io.github.retz.misc.Pair;
import io.github.retz.scheduler.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Optional;

public class RetzJmxServer {
    private static final Logger LOG = LoggerFactory.getLogger(RetzJmxServer.class);

    public static Optional<JmxServer> startJmxServer(ServerConfiguration config, List<Pair<Object, String>> beans) {
        int jmxPort = config.getJmxPort();

        try {
            JmxServer jmxServer = new JmxServer(jmxPort);

            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

            // Registering self
            ObjectName name = new ObjectName("io.github.retz.scheduler:type=AdminConsole");
            AdminConsole mbean = new AdminConsole(config.getGcLeeway());
            mbs.registerMBean(mbean, name);

            for (Pair<Object, String> pair: beans) {
                ObjectName objectName = new ObjectName(pair.right());
                mbs.registerMBean(pair.left(), objectName);
            }

            jmxServer.start();
            LOG.info("JMX enabled listening to {}", jmxPort);
            return Optional.of(jmxServer);

        } catch (JMException e) {
            LogUtil.error(LOG, "RetzJmxServer.startJmxServer() failed", e);
        }
        return Optional.empty();
    }
}
