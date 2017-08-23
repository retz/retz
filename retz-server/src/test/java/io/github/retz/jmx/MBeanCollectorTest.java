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

import com.j256.simplejmx.client.JmxClient;
import com.j256.simplejmx.server.JmxServer;
import io.github.retz.db.Database;
import io.github.retz.scheduler.Launcher;
import io.github.retz.scheduler.ServerConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.ObjectName;
import java.io.InputStream;
import java.util.Optional;

import static org.junit.Assert.*;

public class MBeanCollectorTest {
    private JmxServer jmxServer;
    private int port;

    @Before
    public void before() throws Exception {
        InputStream in = Launcher.class.getResourceAsStream("/retz-tls.properties");

        ServerConfiguration config = new ServerConfiguration(in);
        Database.getInstance().init(config);
        Optional<JmxServer> server = RetzJmxServer.startJmxServer(config, MBeanCollector.beans());
        port = config.getJmxPort();
        jmxServer = server.get();
    }

    @After
    public void after() throws Exception {
        Database.getInstance().clear();
        Database.getInstance().stop();
        jmxServer.stop();
    }

    @Test
    public void smoke() throws Exception {
        JmxClient client = new JmxClient("localhost", port);
        assertNotEquals(0, client.getAttributesInfo(new ObjectName(MBeanCollector.JMX_NAME_STATUS)).length);
        assertNotEquals(0, client.getAttributesInfo(new ObjectName(MBeanCollector.JMX_NAME_TOTAL_OFFERED)).length);
        assertNotEquals(0, client.getAttributesInfo(new ObjectName(MBeanCollector.JMX_NAME_TOTAL_USED)).length);
    }
}
