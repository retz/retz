package io.github.retz.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.retz.protocol.data.Job;

import java.util.Objects;

public class PutRunningRequest extends Request {
    private String taskId;
    private Job job;

    @JsonCreator
    public PutRunningRequest(@JsonProperty(value = "taskId", required = true) String taskId,
                              @JsonProperty(value = "job", required = true) Job job) {
        this.job = Objects.requireNonNull(job);
        this.taskId = Objects.requireNonNull(taskId);
    }

    @JsonGetter("taskId")
    public String taskId() {
        return taskId;
    }
    
    @JsonGetter("job")
    public Job job() {
        return job;
    }

    @Override
    public String resource() {
        return "/running/put";
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
        return "/running/put";
    }
}
