package io.github.retz.protocol.converter;

import io.github.retz.grpcgen.Application;
import io.github.retz.grpcgen.Container;
import io.github.retz.grpcgen.JobState;
import io.github.retz.grpcgen.MountMode;
import io.github.retz.protocol.data.DockerContainer;
import io.github.retz.protocol.data.DockerVolume;
import io.github.retz.protocol.data.Job;
import io.github.retz.protocol.data.MesosContainer;

import java.util.stream.Collectors;

// TODO: write ScalaCheck property test for this kind of conversion
public class Retz2Pb {

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
