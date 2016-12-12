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
package io.github.retz.auth;

import io.github.retz.protocol.ProtocolTest;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProtocolTesterTest {

    void testVersions(String server, String client, boolean ok) {
        if (ok) {
            assertTrue(new ProtocolTester(server, client).validate());
        } else {
            assertFalse(new ProtocolTester(server, client).validate());
        }
    }

    @Test
    public void test() {
        {
            String server = "retz-server-0.1.2 (0.1.1-15-g6341986)";
            String client = "retz-client-0.1.2 (0.1.1-15-g6341986)";
            testVersions(server, client ,true);
        }
        {
            String server = "retz-server-0.1.2-SNAPSHOT (0.1.1-15-g6341986)";
            String client = "retz-client-0.1.2-SNAPSHOT (0.1.1-15-g6341986)";
            testVersions(server, client, true);
        }
        {
            String server = "retz-server-0.2.2-SNAPSHOT (0.1.1-15-g6341986)";
            String client = "retz-client-0.1.2 (0.1.1-15-g6341986)";
            testVersions(server, client, false);

        }
    }
}
