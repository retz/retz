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
package io.github.retz.scheduler;

import io.github.retz.mesos.Resource;
import io.github.retz.protocol.data.Container;
import io.github.retz.protocol.data.DockerContainer;
import org.apache.commons.io.FilenameUtils;
import org.apache.mesos.Protos;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Applications {

    private static final Map<String, Application> DICTIONARY = new ConcurrentHashMap<>();

    private static Map<String, Application> get() {
        return DICTIONARY;
    }

    public static Optional<Application> get(String appName) {
        return Optional.ofNullable(get().get(appName));
    }

    public static List<String> volumes(String role) {
        return (DICTIONARY.entrySet().stream()
                .map(entry -> entry.getValue().toVolumeId(role))
                .collect(Collectors.toList()));
    }

    public static boolean load(io.github.retz.protocol.data.Application app) {
        Application application = new Application();
        // TODO: eliminate all Applications$Application objects and use protocol.Application
        application.appName = Objects.requireNonNull(app.getAppid());
        application.persistentFiles = Objects.requireNonNull(app.getPersistentFiles());
        application.largeFiles = Objects.requireNonNull(app.getLargeFiles());
        application.appFiles = Objects.requireNonNull(app.getFiles());
        application.diskMB = Objects.requireNonNull(app.getDiskMB());
        application.container = Objects.requireNonNull(app.container());

        DICTIONARY.putIfAbsent(application.appName, application);
        return true;
    }

    public static io.github.retz.protocol.data.Application encodable(Application app) {
        io.github.retz.protocol.data.Application encodable = new io.github.retz.protocol.data.Application(
                app.appName, app.persistentFiles, app.largeFiles, app.appFiles, app.diskMB, app.container);
        return encodable;
    }

    public static void unload(String appName) {
        get().remove(appName);
        // Volumes are destroyed lazily
    }

    public static List<Application> getAll() {
        List<Application> apps = new LinkedList<>();
        apps.addAll(get().values());
        return apps;
    }

    public static List<Application> needsPersistentVolume(Resource resource, String role) {
        List<Application> apps = DICTIONARY.entrySet().stream()
                .filter(entry -> !entry.getValue().persistentFiles.isEmpty())
                .filter(entry -> entry.getValue().diskMB.isPresent())
                .filter(entry -> !resource.volumes().containsKey(entry.getValue().toVolumeId(role)))
                .map(entry -> entry.getValue())
                .collect(Collectors.toList());
        return apps;
    }


    public static class Application {
        public String appName;
        public List<String> persistentFiles;
        public List<String> largeFiles;
        public List<String> appFiles;
        public Optional<Integer> diskMB;
        public Container container;

        public Protos.ContainerInfo toContainerInfo() {
            DockerContainer c = (DockerContainer) container;
            return Protos.ContainerInfo.newBuilder().setDocker(
                    Protos.ContainerInfo.DockerInfo.newBuilder()
                            .setImage(c.image()))
                    .setType(Protos.ContainerInfo.Type.DOCKER)
                    .build();
        }
        
        public Protos.CommandInfo toCommandInfo(String command) {
            Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder()
                    .setShell(true)
                    .setValue(command);

            for (String file : appFiles) {
                commandInfoBuilder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(false));
            }
            for (String file : largeFiles) {
                commandInfoBuilder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(true));
            }
            return commandInfoBuilder.build();
        }

        public Protos.ExecutorInfo toExecutorInfo(Protos.FrameworkID frameworkID) {

            String jarFile = FilenameUtils.getName(RetzScheduler.getJarUri());
            String cmd = "java -jar " + jarFile;
            Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder()
                    .setEnvironment(Protos.Environment.newBuilder()
                            .addVariables(Protos.Environment.Variable.newBuilder()
                                    .setName("ASAKUSA_HOME").setValue(".").build()))
                    .setValue(cmd)
                    .setShell(true)
                    .addUris(Protos.CommandInfo.URI.newBuilder().setValue(RetzScheduler.getJarUri()).setCache(false)); // In production, set this true

            for (String file : appFiles) {
                commandInfoBuilder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(false));
            }
            for (String file : largeFiles) {
                commandInfoBuilder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(true));
            }

            Protos.ExecutorInfo executorInfo = Protos.ExecutorInfo.newBuilder()
                    .setCommand(commandInfoBuilder.build())
                    .setExecutorId(Protos.ExecutorID.newBuilder().setValue(appName).build())
                    .setFrameworkId(frameworkID)
                    .build();
            return executorInfo;
        }

        public String toVolumeId(String role) {
            return io.github.retz.protocol.data.Application.toVolumeId(appName);
        }
    }
}
