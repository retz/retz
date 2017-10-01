package io.github.retz.protocol.converter;

import io.github.retz.grpcgen.JobState;
import io.github.retz.protocol.data.Job;

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
}
