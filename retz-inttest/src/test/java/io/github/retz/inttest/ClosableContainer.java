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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.StartContainerCmd;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import io.github.retz.auth.NoopAuthenticator;
import io.github.retz.web.Client;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ClosableContainer implements AutoCloseable {
    private final DockerClient dockerClient;
    private final String containerId;
    private Thread mesosAgentHolder;
    private Thread retzServerHolder;
    private String configfile = "retz.properties";

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
        if (mesosAgentHolder != null && mesosAgentHolder.isAlive()) {
            mesosAgentHolder.join(1000);
        }
        if (retzServerHolder != null && retzServerHolder.isAlive()) {
            retzServerHolder.join(1000);
        }
    }

    public String getId() {
        return this.containerId;
    }

    public String getConfigfile() {
        return configfile;
    }

    public void setConfigfile(String file) {
        this.configfile = file;
    }

    /**
     * Starts container that includes mesos master and agents as well as retz-server.
     * <p>
     * Steps:
     * 1. Start Docker container. A mesos master process starts at this point
     * by SystemD.
     * 2. Wait for "/build/libs/retz-srver-all.jar" is available.
     * Not sure the root cause, but sometimes the file is not visible in the container
     * 3. Then, spawn a mesos agent and a retz server.
     * 4. Wait for these two processes are up by "ps -awxx"
     * 5. Wait for retz server WS interface is ready
     *
     * @throws InterruptedException
     * @throws UnsupportedEncodingException
     */
    public void start() throws Exception {
        System.err.println("Waiting for Retz server start");
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

        startMesosServer();
        startRetzServer(configfile);

        waitFor(
                () -> {
                    String res = ps();
                    return res.contains("/sbin/mesos-slave --master") &&
                            res.contains("retz-server-all.jar");
                },
                "Processes failed to spawn, timed out.",
                () -> {
                    return ps();
                });

        waitForRetzServer();
    }

    private void waitFor(Callable<Boolean> validator, String errorMessage,
                         Callable<String> whenFail) throws Exception {
        int retries = 200;
        while (true) {
            try {
                if (validator.call()) return;
            } catch (Exception e) {
                // nop
            }
            retries--;
            if (retries == 0) {
                System.err.println(errorMessage);
                String dyingMessage = whenFail.call();
                System.err.println(dyingMessage);
                throw new RuntimeException(errorMessage);
            }
            Thread.sleep(100);
        }
    }

    void startMesosServer() {
        System.err.println("Starting mesos server");
        String[] command = {"/spawn_mesos_agent.sh"};
        mesosAgentHolder = new Thread(createHolderTask("mesos-agent-holder", command));
        mesosAgentHolder.start();
    }

    void startRetzServer(String configfile) {
        System.err.println("Starting retz server with " + configfile);
        String[] command = {"/spawn_retz_server.sh", configfile};
        retzServerHolder = new Thread(createHolderTask("retz-server-holder", command));
        retzServerHolder.start();
    }

    void waitForRetzServer() throws Exception {
        URI uri = new URI("http://" + IntTestBase.RETZ_HOST + ":" + IntTestBase.RETZ_PORT);
        System.err.println(uri.toASCIIString());
        waitFor(
                () -> {
                    try (Client client = Client.newBuilder(uri).setAuthenticator(new NoopAuthenticator("anon")).build()) {
                        return client.ping();
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                },
                "HTTP connection could not be established, timed out.",
                () -> {
                    return "retz-server URI: " + uri;
                });
    }

    // Returns true if successfully process killed
    boolean killRetzServerProcess() throws InterruptedException, UnsupportedEncodingException {
        int pid = getRetzServerPid();

        ExecCreateCmdResponse checkPs1 = dockerClient.execCreateCmd(containerId).withAttachStdout(true)
                .withAttachStderr(true).withCmd("kill", Integer.toString(pid)).exec();
        dockerClient.execStartCmd(checkPs1.getId()).withDetach(false)
                .exec(new ExecStartResultCallback(System.out, System.err)).awaitCompletion();

        Thread.sleep(3 * 1024); // 3 seconds rule
        String ps = ps();
        return !ps.contains("retz-server-all.jar");
    }

    public String system(String[] command) throws Exception {
        ExecCreateCmdResponse spawnAgent = dockerClient
                .execCreateCmd(containerId).withTty(true)
                .withAttachStdout(true).withAttachStderr(true)
                .withCmd(command).exec();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        dockerClient.execStartCmd(spawnAgent.getId()).withDetach(false)
                .exec(new ExecStartResultCallback(out, System.err))
                .awaitCompletion(8, TimeUnit.SECONDS);
        return out.toString(String.valueOf(StandardCharsets.UTF_8));
    }

    private Runnable createHolderTask(String name, String[] command) {
        return new Runnable() {
            public void run() {
                try {
                    ExecCreateCmdResponse spawnAgent = dockerClient
                            .execCreateCmd(containerId).withTty(false)
                            .withAttachStdout(false).withAttachStderr(false)
                            .withCmd(command).exec();
                    dockerClient.execStartCmd(spawnAgent.getId()).withDetach(true)
                            .exec(new ExecStartResultCallback(System.out, System.err));
                    System.err.println("Thread: " + name + " finished. Id=" + spawnAgent.getId());
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

    public int getRetzServerPid() throws InterruptedException, UnsupportedEncodingException {
        String ps = ps();
        String[] lines = ps.split("\\r?\\n");
        for (String line : lines) {
            if (!line.contains("retz-server-all.jar")) {
                continue;
            }
            String[] words = line.split("\\s");
            for (String word : words) {
                if (word.isEmpty()) continue;
                try {
                    return Integer.parseInt(word);
                } catch (Exception e) {
                    System.out.println("> " + word);
                }
            }
            return -1;
        }
        return -1;
    }

    static void removeIfExists(DockerClient dockerClient, String containerName) throws InterruptedException {
        List<Container> containers = dockerClient.listContainersCmd().exec();
        for (Container container : containers) {
            for (String name : container.getNames()) {
                System.out.println("name:" + name);
                if (name.endsWith(containerName)) {
                    new ClosableContainer(dockerClient, container.getId()).close();
                    return;
                }
            }
        }
    }
}
