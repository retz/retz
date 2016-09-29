package io.github.retz.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.retz.protocol.data.Job;

import java.util.Objects;

public class PushJobQueueRequest extends Request {
    private Job job;

    @JsonCreator
    public PushJobQueueRequest(@JsonProperty(value = "job", required = true) Job job) {
        this.job = Objects.requireNonNull(job);
    }

    @JsonGetter("job")
    public Job job() {
        return job;
    }

    @Override
    public String resource() {
        return "/job_queue/push";
    }

    @Override
    public String method() {
        return PUT;
    }

    @Override
    public boolean hasPayload() {
        return true;
    }

    public static String resourcePattern() {
        return "/job_queue/push";
    }
}
