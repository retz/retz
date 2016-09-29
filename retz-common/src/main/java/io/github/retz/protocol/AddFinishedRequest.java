package io.github.retz.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.retz.protocol.data.Job;

import java.util.Objects;

public class AddFinishedRequest extends Request {
    private Job job;

    @JsonCreator
    public AddFinishedRequest(@JsonProperty(value = "job", required = true) Job job) {
        this.job = Objects.requireNonNull(job);
    }

    @JsonGetter("job")
    public Job job() {
        return job;
    }

    @Override
    public String resource() {
        return "/finished/add";
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
        return "/finished/add";
    }
}
