package io.github.retz.inttest;

import io.github.retz.cli.ClientCLIConfig;
import io.github.retz.protocol.LoadAppResponse;
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
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.Scanner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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

    @Test
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

            String cmd = "dd if=/dev/urandom of=binary count=1024 bs=16 && tar czf binary.tgz binary && md5sum binary.tgz > binary.tgz.md5";
            Job job = new Job(appName, cmd, new Properties(), 2, 256, 0, 0);
            Job runRes = client.run(job);
            assertThat(runRes.result(), is(RES_OK));
            assertThat(runRes.state(), is(Job.JobState.FINISHED));

            String toDir = "/tmp/";
            ClientHelper.getWholeFile(client, runRes.id(), "binary.tgz", toDir);
            ClientHelper.getWholeFile(client, runRes.id(), "binary.tgz.md5", toDir);

            String remote = readMD5file(toDir + "binary.tgz.md5");

            ProcessBuilder pb = new ProcessBuilder()
                    .command(Arrays.asList("md5sum", "/tmp/binary.tgz"))
                    .redirectErrorStream(true)
                    .redirectOutput(new File("/tmp/binary.tgz.md5.2"));

            assertEquals(0, pb.start().waitFor());

            String local = readMD5file("/tmp/binary.tgz.md5.2");

            assertEquals("Match remote and local MD5 checksum?", remote, local);
        }
    }

    private String readMD5file(String filename) throws IOException {
        try (FileInputStream in = new FileInputStream(filename);
             Scanner scanner = new Scanner(in)) {
            if (scanner.hasNext()) {
                return scanner.next();
            }
        }
        throw new IOException("Empty file: " + filename);
    }
}
