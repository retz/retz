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
package io.github.retz.inttest;

import io.github.retz.cli.ClientCLIConfig;
import io.github.retz.protocol.*;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;
import io.github.retz.web.Client;
import io.github.retz.web.ClientHelper;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class RegressionTest extends IntTestBase {

    // From H2OnMemTest
    @BeforeClass
    public static void setupContainer() throws Exception {
        // Starting containers right here as configurations are static
        setupContainer("retz.properties", false);
    }

    // From H2OnMemTest
    @Override
    ClientCLIConfig makeClientConfig() throws Exception {
        return new ClientCLIConfig("src/test/resources/retz-c.properties");
    }

    @Test // For GH118
    public void binaryDownloadTest() throws Exception {
        URI uri = new URI("http://" + RETZ_HOST + ":" + RETZ_PORT);
        try (Client client = Client.newBuilder(uri)
                .setAuthenticator(config.getAuthenticator())
                .build()) {

            String appName = "generate-binary";
            Application genbin = new Application(appName, Arrays.asList(), Arrays.asList(),
                    Optional.empty(), "deadbeef", 0, new MesosContainer(), true);
            LoadAppResponse loadRes = (LoadAppResponse) client.load(genbin);
            assertThat(loadRes.status(), is("ok"));

            String cmd = "mkdir d && dd if=/dev/urandom of=d/binary count=1024 bs=16 && tar czf d/binary.tgz d/binary && md5sum d/binary.tgz > binary.tgz.md5";
            Job job = new Job(appName, cmd, new Properties(), 2, 256, 32, 0, 0);
            Job runRes = client.run(job);
            assertThat(runRes.result(), is(RES_OK));
            assertThat(runRes.state(), is(Job.JobState.FINISHED));

            {
                ListFilesResponse res = (ListFilesResponse) client.listFiles(runRes.id(), ListFilesRequest.DEFAULT_SANDBOX_PATH);
                List<String> files = res.entries().stream().map(e -> e.path()).collect(Collectors.toList());
                System.err.println(String.join("\n", files));
                assertTrue(files.get(0).endsWith("binary.tgz.md5"));
                assertTrue(files.get(1).endsWith("d"));
                assertTrue(files.get(2).endsWith("stderr"));
                assertTrue(files.get(3).endsWith("stdout"));
            }

            String toDir = "/tmp";
            ClientHelper.getWholeFile(client, runRes.id(), "binary.tgz.md5", toDir);
            String remote = readMD5file(toDir + "/binary.tgz.md5");
            assertFalse(remote.isEmpty());

            ClientHelper.getWholeBinaryFile(client, runRes.id(), "d/binary.tgz", toDir);

            ProcessBuilder pb = new ProcessBuilder()
                    .command(Arrays.asList("md5sum", "/tmp/binary.tgz"))
                    .redirectErrorStream(true)
                    .redirectOutput(new File("/tmp/binary.tgz.md5.2"));

            assertEquals(0, pb.start().waitFor());

            String local = readMD5file("/tmp/binary.tgz.md5.2");
            assertFalse(local.isEmpty());

            assertEquals("Match remote and local MD5 checksum?", remote, local);
        }
    }

    private String readMD5file(String filename) throws IOException {
        try (FileInputStream in = new FileInputStream(filename);
             Scanner scanner = new Scanner(in, "UTF-8")) {
            if (scanner.hasNext()) {
                return scanner.next();
            }
        }
        throw new IOException("Empty file: " + filename);
    }
}
