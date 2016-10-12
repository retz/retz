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
import io.github.retz.protocol.data.DockerVolume;
import org.apache.commons.io.FilenameUtils;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Applications {
    private static final Logger LOG = LoggerFactory.getLogger(Applications.class);

    public static Optional<Application> get(String appName) {
        try {
            return Database.getApplication(appName);
        } catch (IOException e) {
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

        Protos.ContainerInfo.Builder builder = Protos.ContainerInfo.newBuilder().setMesos(
                Protos.ContainerInfo.MesosInfo.newBuilder()
                        .setImage(Protos.Image.newBuilder()
                                .setType(Protos.Image.Type.DOCKER)
                                .setDocker(Protos.Image.Docker.newBuilder()
                                        .setName(c.image()))))
                .setType(Protos.ContainerInfo.Type.MESOS);

        for (DockerVolume volume : c.volumes()) {
            Protos.Volume.Source.DockerVolume.Builder dvb = Protos.Volume.Source.DockerVolume.newBuilder()
                    .setDriver(volume.driver()) // like "nfs"
                    .setName(volume.name()); // like "192.168.204.222/"
            Protos.Parameters.Builder p = Protos.Parameters.newBuilder();
            for (Object key : volume.options().keySet()) {
                p.addParameter(Protos.Parameter.newBuilder()
                        .setKey((String) key)
                        .setValue(volume.options().getProperty((String) key))
                        .build());
            }
            if (!p.getParameterList().isEmpty()) {
                dvb.setDriverOptions(p.build());
            }

            Protos.Volume.Builder vb = Protos.Volume.newBuilder()
                    .setContainerPath(volume.containerPath()) // target path to mount inside container
                    .setSource(Protos.Volume.Source.newBuilder()
                            .setType(Protos.Volume.Source.Type.DOCKER_VOLUME)
                            .setDockerVolume(dvb.build()));

            switch (volume.mode()) {
                case RW:
                    vb.setMode(Protos.Volume.Mode.RW);
                    break;
                case RO:
                default:
                    vb.setMode(Protos.Volume.Mode.RO);
            }
            builder.addVolumes(vb);
        }

        return builder.build();
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


    static List<Protos.Offer.Operation> persistentVolumeOps(Resource resources, Protos.FrameworkInfo frameworkInfo) {
        List<Protos.Offer.Operation> operations = new LinkedList<>();

        List<Application> apps = needsPersistentVolume(resources, frameworkInfo.getRole());
        for (Application a : apps) {
            LOG.debug("Application {}: {} {} volume: {} {}MB",
                    a.getAppid(), String.join(" ", a.getPersistentFiles()),
                    String.join(" ", a.getFiles()), a.toVolumeId(frameworkInfo.getRole()), a.getDiskMB());
        }
        int reservedSpace = resources.reservedDiskMB();
        LOG.debug("Number of volumes: {}, reserved space: {}", resources.volumes().size(), reservedSpace);

        for (Application app : apps) {

            String volumeId = app.toVolumeId();
            LOG.debug("Application {} needs {} MB persistent volume", app.getAppid(), app.getDiskMB());
            if (reservedSpace < app.getDiskMB().get()) {
                // If enough space is not reserved, then reserve
                // TODO: how it must behave when we only have too few space to reserve
                Protos.Resource.Builder rb = baseResourceBuilder(app.getDiskMB().get() - reservedSpace,
                        frameworkInfo);

                LOG.info("Reserving resource with principal={}, role={}, app={}",
                        frameworkInfo.getPrincipal(), frameworkInfo.getRole(), app.getAppid());
                operations.add(Protos.Offer.Operation.newBuilder()
                        .setType(Protos.Offer.Operation.Type.RESERVE)
                        .setReserve(Protos.Offer.Operation.Reserve.newBuilder()
                                .addResources(rb.build())
                        )
                        .build());
            } else if (!resources.volumes().containsKey(volumeId)) {
                // We have enough space to reserve, then do it
                Protos.Resource.Builder rb = baseResourceBuilder(app.getDiskMB().get(), frameworkInfo);
                LOG.info("Creating {} MB volume {} for application {}", app.getDiskMB(), volumeId, app.getAppid());
                operations.add(Protos.Offer.Operation.newBuilder()
                        .setType(Protos.Offer.Operation.Type.CREATE)
                        .setCreate(Protos.Offer.Operation.Create.newBuilder().addVolumes(
                                rb.setDisk(Protos.Resource.DiskInfo.newBuilder()
                                        .setPersistence(Protos.Resource.DiskInfo.Persistence.newBuilder()
                                                .setPrincipal(frameworkInfo.getPrincipal())
                                                .setId(volumeId))
                                        .setVolume(Protos.Volume.newBuilder()
                                                .setMode(Protos.Volume.Mode.RW)
                                                .setContainerPath(app.getAppid() + "-home")))
                                        .build())
                        ).build());
                reservedSpace -= app.getDiskMB().get();
            }
        }
        return operations;
    }

    private static Protos.Resource.Builder baseResourceBuilder(int diskMB, Protos.FrameworkInfo frameworkInfo) {
        return Protos.Resource.newBuilder()
                .setName("disk")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(diskMB))
                .setType(Protos.Value.Type.SCALAR)
                .setReservation(Protos.Resource.ReservationInfo.newBuilder()
                        .setPrincipal(frameworkInfo.getPrincipal()))
                .setRole(frameworkInfo.getRole());
    }

    static List<Protos.Offer.Operation> persistentVolumeCleanupOps(Resource resources, Protos.FrameworkInfo frameworkInfo) {
        List<Protos.Offer.Operation> operations = new LinkedList<>();
        List<String> usedVolumes = Applications.volumes(frameworkInfo.getRole());
        Map<String, Protos.Resource> unusedVolumes = new LinkedHashMap<>();
        for (Map.Entry<String, Protos.Resource> volume : resources.volumes().entrySet()) {
            if (!usedVolumes.contains(volume.getKey())) {
                unusedVolumes.put(volume.getKey(), volume.getValue());
            }
        }
        for (Map.Entry<String, Protos.Resource> volume : unusedVolumes.entrySet()) {
            String volumeId = volume.getKey();
            LOG.info("Destroying {}", volumeId);
            operations.add(Protos.Offer.Operation.newBuilder()
                    .setType(Protos.Offer.Operation.Type.DESTROY)
                    .setDestroy(Protos.Offer.Operation.Destroy.newBuilder().addVolumes(volume.getValue()))
                    .build());
            LOG.info("Unreserving {}", volumeId);
            operations.add(Protos.Offer.Operation.newBuilder()
                    .setType(Protos.Offer.Operation.Type.UNRESERVE)
                    .setUnreserve(Protos.Offer.Operation.Unreserve.newBuilder()
                            .addResources(Protos.Resource.newBuilder()
                                    .setReservation(Protos.Resource.ReservationInfo.newBuilder()
                                            .setPrincipal(frameworkInfo.getPrincipal()))
                                    .setName("disk")
                                    .setType(Protos.Value.Type.SCALAR)
                                    .setScalar(resources.volumes().get(volumeId).getScalar())
                                    .setRole(frameworkInfo.getRole())
                                    .build())
                    ).build());
        }
        return operations;
    }
}

