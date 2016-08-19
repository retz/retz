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
package io.github.retz.inttest;

import io.github.retz.web.Client;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import com.github.dockerjava.netty.DockerCmdExecFactoryImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Base class for Retz integration testing.
 */
public class IntTestBase {
    public static final String IMAGE_NAME = "mesos-retz";

    // To execute tests parallel, names and ports should be dynamic.
    // It's not necessary to name containers, except manual cleanup.
    public static final String CONTAINER_NAME = "retz-inttest-ertpqgh34jv9air";
    public static final int RETZ_PORT = 19090;

    // Probably better to make log and downloading directories for testc ases.
    private static String hostBuildDir;

    private static ClosableContainer container;

    @BeforeClass
    public static void setupContainer() throws Exception {
        hostBuildDir = new File("./build/").getCanonicalPath();
        boolean _res = new File(hostBuildDir, "log/").mkdirs();

        DockerClientConfig.DockerClientConfigBuilder builder
                = DockerClientConfig.createDefaultConfigBuilder().withApiVersion("1.12");
        Optional<String> dockerHostEnv = Optional.ofNullable(System.getenv("DOCKER_HOST"));
        builder.withDockerHost(dockerHostEnv.orElse("unix:///var/run/docker.sock"));

        DockerClientConfig config = builder.build();
        DockerClientBuilder dockerClientBuilder = DockerClientBuilder.getInstance(config);
        dockerClientBuilder.withDockerCmdExecFactory(new DockerCmdExecFactoryImpl());

        DockerClient dockerClient = dockerClientBuilder.build();
        removeIfExists(dockerClient, CONTAINER_NAME);

        Ports portBindings = new Ports();
        List<ExposedPort> ports = Arrays.asList(15050, 15051, RETZ_PORT).stream()
                .map(port -> ExposedPort.tcp(port)).collect(Collectors.toList());
        ports.forEach(port -> portBindings.bind(port, Ports.Binding.bindPort(port.getPort())));

        Volume containerBuildDir = new Volume("/build");
        CreateContainerCmd createCmd = dockerClient.createContainerCmd(IMAGE_NAME)
                .withName(CONTAINER_NAME)
                .withPrivileged(true)       // To use SystemD/Cgroups
                .withExposedPorts(ports)
                .withPortBindings(portBindings)
                .withBinds(new Bind(hostBuildDir, containerBuildDir));

        container = ClosableContainer.createContainer(dockerClient, createCmd);
        container.start();

        System.out.println(); System.out.println("====================");
        System.out.println("Processes (by ps -awxx)");
        System.out.println(container.ps());
        System.out.println(); System.out.println("====================");
    }

    @AfterClass
    public static void cleanupContainer() throws Exception {
        container.close();
    }

    public static String retzServerUri() {
        return "ws://127.0.0.1:" + Integer.toString(RETZ_PORT) + "/cui";
    }

    private static void removeIfExists(DockerClient dockerClient, String containerName) throws InterruptedException {
        List<Container> containers = dockerClient.listContainersCmd().exec();
        for(Container container: containers) {
            for (String name : container.getNames()) {
                System.out.println("name:" + name);
                if (name.endsWith(containerName)) {
                    new ClosableContainer(dockerClient, container.getId()).close();
                    return;
                }
            }
        }
    }

    public static class ClosableContainer implements AutoCloseable {
        private final DockerClient dockerClient;
        private final String containerId;
        private Thread mesosAgentHolder;
        private Thread retzServerHolder;

        private ClosableContainer(DockerClient dockerClient, String containerId) {
            this.dockerClient = dockerClient;
            this.containerId = containerId;
        }

        public static ClosableContainer createContainer(DockerClient dockerClient, CreateContainerCmd createCmd) {
            CreateContainerResponse commandResponse = createCmd.exec();
            String containerId = commandResponse.getId();
            return new ClosableContainer(dockerClient, containerId);
        }

        public void close() throws InterruptedException {
            try {
                dockerClient.killContainerCmd(containerId).exec();
            } catch (Exception e) {
                // no-op
            } finally {
                dockerClient.removeContainerCmd(containerId).exec();
            }
            if(mesosAgentHolder != null && mesosAgentHolder.isAlive()){
                mesosAgentHolder.join(1000);
            }
            if(retzServerHolder != null && retzServerHolder.isAlive()) {
                retzServerHolder.join(1000);
            }
        }

        public String getId() {
            return this.containerId;
        }

        /**
         * Starts container that includes mesos master and agents as well as retz-server.
         *
         * Steps:
         * 1. Start Docker container. A mesos master process starts at this point
         *    by SystemD.
         * 2. Wait for "/build/libs/retz-srver-all.jar" is available.
         *    Not sure the root cause, but sometimes the file is not visible in the container
         * 3. Then, spawn mesos slave and retz server.
         * 4. Wait for these two processes are up by "ps -awxx"
         * 5. Wait for retz server WS interface is ready
         *
         * @throws InterruptedException
         * @throws UnsupportedEncodingException
         */
        public void start() throws Exception {
            StartContainerCmd startContainerCmd = dockerClient.startContainerCmd(containerId);
            startContainerCmd.exec();
            waitFor(
                    () -> {
                        String res = listFiles("/build/libs/");
                        return res.contains("retz-server-all.jar");
                    },
                    "Shared \"build\" directory could not be visible.",
                    () -> listFiles("/build/libs/")
            );

            startServers();
            waitFor(
                    () -> {
                        String res = ps();
                        return res.contains("/sbin/mesos-slave --master") &&
                                res.contains("retz-server-all.jar");
                    },
                    "Processes failed to spawn, timed out.",
                    () -> {return ps();});
            waitFor(
                    () -> {
                        Client client = new Client(IntTestBase.retzServerUri());
                        return client.connect();
                    },
                    "WS connection could not be established, timed out.",
                    () -> {return "retz-server URI: " + IntTestBase.retzServerUri();});
        }

        private void waitFor(Callable<Boolean> validator, String errorMessage,
                             Callable<String> whenFail) throws Exception {
            int retries = 50;
            while(true) {
                try {
                    if(validator.call()) return;
                } catch (Exception e) {
                    // nop
                }
                retries--;
                if(retries == 0){
                    System.err.println(errorMessage);
                    String dyingMessage = whenFail.call();
                    System.err.println(dyingMessage);
                    throw new RuntimeException(errorMessage);
                }
                Thread.sleep(100);
            }
        }

        private void startServers() {
            mesosAgentHolder = new Thread(createHolderTask("mesos-agent-holder", "/spawn_mesos_agent.sh"));
            mesosAgentHolder.start();
            retzServerHolder = new Thread(createHolderTask("retz-server-holder", "/spawn_retz_server.sh"));
            retzServerHolder.start();
        }

        private Runnable createHolderTask(String name, String command) {
            return new Runnable() {
                public void run() {
                    try {
                        ExecCreateCmdResponse spawnAgent = dockerClient
                                .execCreateCmd(containerId).withTty(false)
                                .withAttachStdout(false).withAttachStderr(false)
                                .withCmd(command).exec();
                        dockerClient.execStartCmd(spawnAgent.getId()).withDetach(true)
                                .exec(new ExecStartResultCallback(System.out, System.err));
                        System.out.println("Thread: " + name + " finished.");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        private String listFiles(String dir) throws InterruptedException, UnsupportedEncodingException {
            ExecCreateCmdResponse checkLs1 = dockerClient.execCreateCmd(containerId).withAttachStdout(true)
                    .withAttachStderr(true).withCmd("ls", "-l", dir).exec();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            dockerClient.execStartCmd(checkLs1.getId()).withDetach(false)
                    .exec(new ExecStartResultCallback(out, System.err)).awaitCompletion();
            return out.toString(String.valueOf(StandardCharsets.UTF_8));
        }

        public String ps() throws InterruptedException, UnsupportedEncodingException {
            ExecCreateCmdResponse checkPs1 = dockerClient.execCreateCmd(containerId).withAttachStdout(true)
                    .withAttachStderr(true).withCmd("ps", "-awxx").exec();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            dockerClient.execStartCmd(checkPs1.getId()).withDetach(false)
                    .exec(new ExecStartResultCallback(out, System.err)).awaitCompletion();
            return out.toString(String.valueOf(StandardCharsets.UTF_8));
        }

    }
}
