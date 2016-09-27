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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.github.retz.mesos.Resource;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Container;
import io.github.retz.protocol.data.DockerContainer;
import org.apache.commons.io.FilenameUtils;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Applications {
    private static final Logger LOG = LoggerFactory.getLogger(Applications.class);

    public static Optional<Application> get(String appName) {
        try {
            return Database.getApplication(appName);
        } catch (IOException e){
            LOG.error(e.toString());
            return Optional.empty();
        }
    }

    public static List<String> volumes(String role) {
        try {
            return (Database.getAllApplications().stream()
                    .map(entry -> entry.toVolumeId(role))
                    .collect(Collectors.toList()));
        } catch (IOException e) {
            LOG.error("Volumes could not be found {}: returning empty list ..", e.toString());
            return new LinkedList<>();
        }
    }

    public static boolean load(Application application) {
        try {
            return Database.addApplication(application);
        } catch (JsonProcessingException e) {
            return false; // Maybe this must be handled inside addApplication...
        }
    }

    public static void unload(String appName) {
        // Volumes are destroyed lazily
        LOG.info("deleting {}", appName);
        Database.safeDeleteApplication(appName);
    }

    public static List<Application> getAll() {
        try {
            return Database.getAllApplications();
        } catch (IOException e) {
            LOG.error(e.toString());
            return new LinkedList<>();
        }
    }

    // TODO: this might be very heavy proportional to number of applications, which could be
    // cut out to another thread, or even remove persistent volume support?
    public static List<Application> needsPersistentVolume(Resource resource, String role) {
        List<Application> apps = getAll().stream()
                .filter(entry -> !entry.getPersistentFiles().isEmpty())
                .filter(entry -> entry.getDiskMB().isPresent())
                .filter(entry -> !resource.volumes().containsKey(entry.toVolumeId(role)))
                .collect(Collectors.toList());
        return apps;
    }


    public static Protos.ContainerInfo appToContainerInfo(Application application) {
        Container container = application.container();
        DockerContainer c = (DockerContainer) container;

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

