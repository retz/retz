package io.github.retz.protocol.converter;

import io.github.retz.grpcgen.JobState;
import io.github.retz.grpcgen.MountMode;
import io.github.retz.protocol.data.*;

import java.util.*;
import java.util.stream.Collectors;

// set of converters from PB3 to Retz native data structure
public class Pb2Retz {

    public static DockerVolume convert(io.github.retz.grpcgen.DockerVolume gDockerVolume) {
        DockerVolume.Mode mode = DockerVolume.Mode.RO;
        switch (gDockerVolume.getModeValue()) {
            case MountMode.RO_VALUE:
                mode = DockerVolume.Mode.RO;
                break;
            case MountMode.RW_VALUE:
                mode = DockerVolume.Mode.RW;
                break;
        }

        Properties props = new Properties();
        props.putAll(gDockerVolume.getOptionsMap());
        return new DockerVolume(gDockerVolume.getDriver(),
                gDockerVolume.getContainerPath(), mode,
                gDockerVolume.getName(), props);
    }

    public static DockerContainer convert(io.github.retz.grpcgen.DockerContainer gDockerContainer) {
        return new DockerContainer(
                gDockerContainer.getImage(),
                gDockerContainer.getVolumesList().stream()
                        .map(volume -> convert(volume)).collect(Collectors.toList()));
    }

    public static MesosContainer convert(io.github.retz.grpcgen.MesosContainer gMesosContainer) {
        return new MesosContainer();
    }

    public static Container convert(io.github.retz.grpcgen.Container gContainer) {
        if (gContainer.hasDockerContainer()) {
            return convert(gContainer.getDockerContainer());
        } else if (gContainer.hasMesosContainer()) {
            return convert(gContainer.getMesosContainer());
        }
        throw new AssertionError("Cannot reach here: " + gContainer);
    }

    public static Application convert(io.github.retz.grpcgen.Application gApp) {
        Container container = convert(gApp.getContainer());
        return new Application(gApp.getAppid(), gApp.getLargeFilesList(), gApp.getFilesList(),
                Optional.ofNullable(gApp.getUser()), gApp.getOwner(), gApp.getGracePeriod(),
                container, gApp.getEnabled());
    }

    public static Job.JobState convert(JobState state) {
        switch (state.getNumber()) {
            case JobState.CREATED_VALUE:
                return Job.JobState.CREATED;
            case JobState.QUEUED_VALUE:
                return Job.JobState.QUEUED;
            case JobState.STARTING_VALUE:
                return Job.JobState.STARTING;
            case JobState.STARTED_VALUE:
                return Job.JobState.STARTED;
            case JobState.FINISHED_VALUE:
                return Job.JobState.FINISHED;
            case JobState.KILLED_VALUE:
                return Job.JobState.KILLED;
            default:
                throw new AssertionError("Cannot reach here:" + state);
        }
    }

    public static Properties convert(Map<String, String> prop) {
        Properties props = new Properties();
        props.putAll(prop);
        return props;
    }

    public static ResourceQuantity convert(io.github.retz.grpcgen.ResourceQuantity gRQ) {
        return new ResourceQuantity(gRQ.getCpu(), gRQ.getMemMB(), gRQ.getGpu(),
                gRQ.getPorts(), gRQ.getDiskMB(), gRQ.getNodes());
    }
    public static Job convert(io.github.retz.grpcgen.Job gJob) {
        Job.JobState state = convert(gJob.getState());
        Set<String> tags = gJob.getTagsList().stream().collect(Collectors.toSet());
        return new Job(gJob.getCmd(),
                gJob.getScheduled(), gJob.getStarted(), gJob.getFinished(),
                convert(gJob.getPropertiesMap()), gJob.getResult(), gJob.getId(),
                gJob.getUrl(), gJob.getReason(), gJob.getRetry(),
                gJob.getPriority(), gJob.getAppid(), gJob.getName(),
                tags, convert(gJob.getResources()),
                Optional.ofNullable(gJob.getAttributes()),
                gJob.getTaskId(), gJob.getSlaveId(), state);
    }
}
