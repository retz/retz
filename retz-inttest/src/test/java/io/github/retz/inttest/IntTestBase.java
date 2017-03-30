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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.netty.DockerCmdExecFactoryImpl;
import io.github.retz.cli.ClientCLIConfig;
import io.github.retz.web.Client;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Base class for Retz integration testing.
 */
public class IntTestBase {
    public static final String IMAGE_NAME = "mesos-retz";

    // To execute tests parallel, names and ports should be dynamic.
    // It's not necessary to name containers, except manual cleanup.
    protected static final String CONTAINER_NAME = "retz-inttest-ertpqgh34jv9air";
    protected static final String RETZ_HOST = "127.0.0.1";
    protected static final int RETZ_PORT = 19090;
    protected static final int MESOS_PORT = 15050;
    // Probably better to make log and downloading directories for testc ases.
    private static String hostBuildDir;

    static int RES_OK = 0;
    private static final String configfile = "retz-c.properties";
    static ClosableContainer container;
    protected ClientCLIConfig config;
    static String serverConfigFile;

    ClientCLIConfig makeClientConfig() throws Exception {
        throw new RuntimeException("This class shouldn't be tested");
    }

    public static ClosableContainer createContainer(String containerName) throws Exception {
        hostBuildDir = new File("./build/").getCanonicalPath();
        boolean _res = new File(hostBuildDir, "log/").mkdirs();

        DefaultDockerClientConfig.Builder builder
                = DefaultDockerClientConfig.createDefaultConfigBuilder().withApiVersion("1.12");
        Optional<String> dockerHostEnv = Optional.ofNullable(System.getenv("DOCKER_HOST"));
        builder.withDockerHost(dockerHostEnv.orElse("unix:///var/run/docker.sock"));

        DockerClientConfig config = builder.build();
        DockerClientBuilder dockerClientBuilder = DockerClientBuilder.getInstance(config);
        dockerClientBuilder.withDockerCmdExecFactory(new DockerCmdExecFactoryImpl());

        DockerClient dockerClient = dockerClientBuilder.build();
        ClosableContainer.removeIfExists(dockerClient, containerName);

        Ports portBindings = new Ports();
        List<ExposedPort> ports = Arrays.asList(15050, 15051, RETZ_PORT, 9999).stream()
                .map(port -> ExposedPort.tcp(port)).collect(Collectors.toList());
        ports.forEach(port -> portBindings.bind(port, Ports.Binding.bindPort(port.getPort())));

        Volume containerBuildDir = new Volume("/build");
        CreateContainerCmd createCmd = dockerClient.createContainerCmd(IMAGE_NAME)
                .withName(containerName)
                .withPrivileged(true)       // To use SystemD/Cgroups
                .withExposedPorts(ports)
                .withPortBindings(portBindings)
                .withBinds(new Bind(hostBuildDir, containerBuildDir));

        return ClosableContainer.createContainer(dockerClient, createCmd);
    }

    protected static void setupContainer(String configFile, boolean needsPostgres) throws Exception {
        System.out.println(Client.VERSION_STRING);
        container = createContainer(CONTAINER_NAME);
        serverConfigFile = configFile;
        System.out.println("Using server config file "+ configFile);
        container.setConfigfile(configFile);

        container.start(needsPostgres);

        System.out.println();
        System.out.println("====================");
        System.out.println("Processes (by ps -awxx)");
        System.out.println(container.ps());
        System.out.println();
        System.out.println("====================");
        System.out.println(container.getRetzServerPid());
    }

    @AfterClass
    public static void cleanupContainer() throws Exception {
        container.close();
    }

    @Before
    public void loadConfig() throws Exception {
        config = makeClientConfig();
        assertEquals(RETZ_HOST, config.getUri().getHost());
        assertEquals(RETZ_PORT, config.getUri().getPort());
        assertNotNull(container);
        assertNotNull(serverConfigFile);
    }
}
