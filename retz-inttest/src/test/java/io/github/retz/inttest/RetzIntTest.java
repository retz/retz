/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, KK.
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
package io.github.retz.inttest;

import io.github.retz.web.Client;
import io.github.retz.protocol.*;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;

/**
 * Simple integration test cases for retz-server / -executor.
 */
public class RetzIntTest extends IntTestBase {
    private static final int RES_OK = 0;

    @Test
    public void listAppTest() throws Exception {
        Client client = new Client(retzServerUri());
        assert(client.connect());
        ListAppResponse response = (ListAppResponse) client.listApp();
        assertEquals("ok", response.status());
        assertEquals(0, response.applicationList().size());
        client.close();
    }

    @Test
    public void runAppTest() throws Exception {
        Client client = new Client(retzServerUri());
        assert(client.connect());
        LoadAppResponse loadRes =
                (LoadAppResponse) client.load("echo-app", Arrays.asList(),
                        Arrays.asList("file:///spawn_retz_server.sh"), null);
        assertEquals("ok", loadRes.status());

        ListAppResponse listRes = (ListAppResponse) client.listApp();
        assertEquals("ok",       listRes.status());
        assertEquals(1,          listRes.applicationList().size());
        assertEquals("echo-app", listRes.applicationList().get(0).getAppid());
        String echoText = "hoge from echo-app via Retz!";
        Job job = new Job("echo-app", "echo " + echoText,
                new Properties(), new Range(1,2), new Range(128, 256));
        Job runRes = client.run(job);
        assertEquals(RES_OK, runRes.result());

        URL resUrl = new URL(runRes.url());
        // Rewrite HOST (IP) part to access without bridge interface in Docker for Mac
        String baseUrl = new URL(resUrl.getProtocol(), "127.0.0.1", resUrl.getPort(), resUrl.getFile()).toString();
        String toDir = "build/log/";
        // These downloaded files are not inspected now, useful for debugging test cases, maybe
        Client.fetchHTTPFile(baseUrl, "stdout", toDir);
        Client.fetchHTTPFile(baseUrl, "stderr", toDir);
        Client.fetchHTTPFile(baseUrl, "stdout-" + runRes.id(), toDir);
        Client.fetchHTTPFile(baseUrl, "stderr-" + runRes.id(), toDir);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Client.catHTTPFile(baseUrl, "stdout-" + runRes.id(), out);
        String actualText = out.toString(String.valueOf(StandardCharsets.UTF_8));
        assertEquals(echoText + "\n", actualText);

        ListJobResponse listJobResponse = (ListJobResponse) client.list(64);
        assertEquals(1, listJobResponse.finished().size());
        assertEquals(0, listJobResponse.running().size());
        assertEquals(0, listJobResponse.queue().size());

        UnloadAppResponse unloadRes = (UnloadAppResponse) client.unload("echo-app");
        assertEquals("ok", unloadRes.status());

        client.close();
    }

}
