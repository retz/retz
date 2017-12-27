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
package io.github.retz.protocol.converter;

import io.github.retz.grpcgen.*;
import io.github.retz.protocol.data.DockerContainer;
import io.github.retz.protocol.data.DockerVolume;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;

import java.util.stream.Collectors;

// TODO: write ScalaCheck property test for this kind of conversion
public class Retz2Pb {

    public static DirEntry convert(io.github.retz.protocol.data.DirEntry dirEntry) {
        return DirEntry.newBuilder()
                .setGid(dirEntry.gid())
                .setMode(dirEntry.mode())
                .setMtime(dirEntry.mtime())
                .setNlink(dirEntry.nlink())
                .setPath(dirEntry.path())
                .setSize(dirEntry.size())
                .setUid(dirEntry.uid())
                .build();
    }

    public static JobState convert(Job.JobState state) {
        switch (state) {
            case CREATED:
                return JobState.CREATED;
            case QUEUED:
                return JobState.QUEUED;
            case STARTING:
                return JobState.STARTING;
            case STARTED:
                return JobState.STARTED;
            case FINISHED:
                return JobState.FINISHED;
            case KILLED:
                return JobState.KILLED;
            default:
                throw new AssertionError("Cannot reach here:" + state);
        }
    }

    public static ResourceQuantity convert(io.github.retz.protocol.data.ResourceQuantity resourceQuantity) {
        ResourceQuantity.Builder builder = ResourceQuantity.newBuilder();
        builder.setCpu(resourceQuantity.getCpu())
                .setDiskMB(resourceQuantity.getDiskMB())
                .setGpu(resourceQuantity.getGpu())
                .setMemMB(resourceQuantity.getMemMB())
                .setNodes(resourceQuantity.getNodes())
                .setPorts(resourceQuantity.getPorts());
        return builder.build();
    }
    public static io.github.retz.grpcgen.Job convert(Job job) {
        io.github.retz.grpcgen.Job.Builder builder = io.github.retz.grpcgen.Job.newBuilder();
        builder.setId(job.id())
                .setPriority(job.priority())
                .setResources(Retz2Pb.convert(job.resources()))
                .setResult(job.result())
                .setRetry(job.retry())
                .setState(Retz2Pb.convert(job.state()))
                .setAppid(job.appid())
                .setCmd(job.cmd())
                .setName(job.name())
                .addAllTags(job.tags());
        if (job.started() != null) {
            builder.setStarted(job.started());
        }
        if (job.finished() != null) {
            builder.setFinished(job.finished());
        }
        if (job.scheduled() != null) {
            builder.setScheduled(job.scheduled());
        }
        if (job.slaveId() != null) {
            builder.setSlaveId(job.slaveId());
        }
        if (job.reason() != null) {
            builder.setReason(job.reason());
        }
        if (job.attributes().isPresent()) {
            builder.setAttributes(job.attributes().get());
        }
        if (job.url() != null) {
            builder.setUrl(job.url());
        }
        if (job.taskId() != null) {
            builder.setTaskId(job.taskId());
        }
        return builder.build();
    }

    public static io.github.retz.grpcgen.DockerVolume convert(DockerVolume dockerVolume) {
        MountMode mode = MountMode.RO;
        switch (dockerVolume.mode()) {
            case RO:
                mode = MountMode.RO;
                break;
            case RW:
                mode = MountMode.RW;
                break;
            default:
                throw new AssertionError("Cannot reach here:" + dockerVolume.mode());
        }
        io.github.retz.grpcgen.DockerVolume.Builder builder = io.github.retz.grpcgen.DockerVolume.newBuilder()
                .setMode(mode)
                .setContainerPath(dockerVolume.containerPath())
                .setDriver(dockerVolume.driver())
                .setName(dockerVolume.name());

        for (String key : dockerVolume.options().stringPropertyNames()) {
            builder.putOptions(key, dockerVolume.options().getProperty(key));
        }
        return builder.build();
    }
    public static io.github.retz.grpcgen.DockerContainer convert(DockerContainer dockerContainer) {
        return io.github.retz.grpcgen.DockerContainer.newBuilder()
                .addAllVolumes(dockerContainer.volumes().stream().map(volume -> convert(volume)).collect(Collectors.toList()))
                .setImage(dockerContainer.image())
                .build();
    }

    public static Application convert(io.github.retz.protocol.data.Application application) {
        Application.Builder builder = Application.newBuilder()
                .setAppid(application.getAppid())
                .addAllLargeFiles(application.getLargeFiles())
                .addAllFiles(application.getFiles());

        if (application.getUser().isPresent()) {
            builder.setUser(application.getUser().get());
        }
        builder.setOwner(application.getOwner())
                .setGracePeriod(application.getGracePeriod());

        Container.Builder containerBuilder = Container.newBuilder();
        if (application.container() instanceof MesosContainer) {
            containerBuilder.setMesosContainer(io.github.retz.grpcgen.MesosContainer.newBuilder());
        } else if (application.container() instanceof DockerContainer) {
            containerBuilder.setDockerContainer(convert((DockerContainer)application.container()));
        } else {
            throw new AssertionError("Cannot reach here:" + application.container().pp());
        }

        builder.setContainer(containerBuilder)
                .setEnabled(application.enabled());
        return builder.build();
    }
}
