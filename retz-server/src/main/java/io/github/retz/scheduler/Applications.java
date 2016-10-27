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
import io.github.retz.db.Database;
import io.github.retz.mesos.Resource;
import io.github.retz.protocol.data.*;
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
            return Database.getInstance().getApplication(appName);
        } catch (IOException e) {
            LOG.error(e.toString());
            return Optional.empty();
        }
    }

    public static boolean load(Application application) {
        try {
            return Database.getInstance().addApplication(application);
        } catch (JsonProcessingException e) {
            return false; // Maybe this must be handled inside addApplication...
        }
    }

    @Deprecated
    public static void unload(String appName) {
        // Volumes are destroyed lazily
        LOG.info("deleting {}", appName);
        Database.getInstance().safeDeleteApplication(appName);
    }

    public static List<Application> getAll() {
        try {
            return Database.getInstance().getAllApplications();
        } catch (IOException e) {
            LOG.error(e.toString());
            return new LinkedList<>();
        }
    }

    public static List<Application> getAll(String id) {
        try {
            return Database.getInstance().getAllApplications(id);
        } catch (IOException e) {
            LOG.error(e.toString());
            return new LinkedList<>();
        }
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

    public static Protos.CommandInfo appToCommandInfo(Application application, Job job) {
        Protos.CommandInfo.Builder builder = Protos.CommandInfo.newBuilder();
        if (application.getUser().isPresent()) {
            builder.setUser(application.getUser().get());
        }
        for (String file : application.getFiles()) {
            builder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(false));
        }
        for (String file : application.getLargeFiles()) {
            builder.addUris(Protos.CommandInfo.URI.newBuilder().setValue(file).setCache(true));
        }
        Protos.Environment.Builder envBuilder = Protos.Environment.newBuilder();
        for (Map.Entry<Object, Object> e : job.props().entrySet()) {
            String key = (String) e.getKey();
            String value = (String) e.getValue();
            envBuilder.addVariables(Protos.Environment.Variable.newBuilder()
                    .setName(key).setValue(value).build());
        }
        return builder.setEnvironment(envBuilder.build())
                .setValue(job.cmd())
                .setShell(true)
                .build();
    }
}

