package io.github.retz.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.retz.protocol.data.Job;

import java.util.Objects;

public class RemoveRunningRequest extends Request {
    private String taskId;

    @JsonCreator
    public RemoveRunningRequest(@JsonProperty(value = "taskId", required = true) String taskId) {
        this.taskId = Objects.requireNonNull(taskId);
    }

    @JsonGetter("taskId")
    public String taskId() {
        return taskId;
    }

    @Override
    public String resource() {
        return "/running/remove/" + taskId;
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
        return "/running/remove/:taskId";
    }
}