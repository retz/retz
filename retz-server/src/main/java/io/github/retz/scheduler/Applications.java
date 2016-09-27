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
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Container;
import io.github.retz.protocol.data.DockerContainer;
import org.apache.commons.io.FilenameUtils;
import org.apache.mesos.Protos;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    public static boolean load(Application application) {
        DICTIONARY.putIfAbsent(application.getAppid(), application);
        return true;
    }

    public static Application encodable(Application app) {
        return app;
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
                .filter(entry -> !entry.getValue().getPersistentFiles().isEmpty())
                .filter(entry -> entry.getValue().getDiskMB().isPresent())
                .filter(entry -> !resource.volumes().containsKey(entry.getValue().toVolumeId(role)))
                .map(entry -> entry.getValue())
                .collect(Collectors.toList());
        return apps;
    }


    public static Protos.ContainerInfo appToContainerInfo(Application application) {
        Container container = application.container();
        DockerContainer c = (DockerContainer) container;
        /*
        return Protos.ContainerInfo.newBuilder().setDocker(
                Protos.ContainerInfo.DockerInfo.newBuilder()
                        .setImage(c.image()))
                .setType(Protos.ContainerInfo.Type.DOCKER)
                .build();
                */
        return Protos.ContainerInfo.newBuilder().setMesos(
                Protos.ContainerInfo.MesosInfo.newBuilder()
                        .setImage(Protos.Image.newBuilder()
                                .setType(Protos.Image.Type.DOCKER)
                                .setDocker(Protos.Image.Docker.newBuilder()
                                        .setName(c.image()))))
                .setType(Protos.ContainerInfo.Type.MESOS)
                .build();
    }

    public static Protos.CommandInfo appToCommandInfo(Application application, String command) {
        Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder()
                .setShell(true)
                .setValue(command);

        if (application.getUser().isPresent()) {
            commandInfoBuilder.setUser(application.getUser().get());
        }
        for (String file : application.getFiles()) {
            commandInfoBuilder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(false));
        }
        for (String file : application.getLargeFiles()) {
            commandInfoBuilder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(true));
        }
        return commandInfoBuilder.build();
    }

    public static Protos.ExecutorInfo appToExecutorInfo(String java, Application application, Protos.FrameworkID frameworkID) {

        String jarFile = FilenameUtils.getName(RetzScheduler.getJarUri());
        String cmd = java + " -jar " + jarFile;
        Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder()
                .setEnvironment(Protos.Environment.newBuilder()
                        .addVariables(Protos.Environment.Variable.newBuilder()
                                .setName("ASAKUSA_HOME").setValue(".").build()))
                .setValue(cmd)
                .setShell(true)
                .addUris(Protos.CommandInfo.URI.newBuilder().setValue(RetzScheduler.getJarUri()).setCache(false)); // In production, set this true

        if (application.getUser().isPresent()) {
            commandInfoBuilder.setUser(application.getUser().get());
        }
        for (String file : application.getFiles()) {
            commandInfoBuilder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(false));
        }
        for (String file : application.getLargeFiles()) {
            commandInfoBuilder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(true));
        }

        Protos.ExecutorInfo executorInfo = Protos.ExecutorInfo.newBuilder()
                .setCommand(commandInfoBuilder.build())
                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(application.getAppid()).build())
                .setFrameworkId(frameworkID)
                .build();
        return executorInfo;
    }
}

