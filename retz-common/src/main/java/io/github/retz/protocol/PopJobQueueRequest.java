package io.github.retz.protocol;

import java.util.Objects;

public class PopJobQueueRequest extends Request {

    public PopJobQueueRequest() {
    }

    @Override
    public String resource() {
        return "/job_queue/pop";
    }

    @Override
    public String method() {
        return PUT;
    }

    @Override
    public boolean hasPayload() {
        return false;
    }

    public static String resourcePattern() {
        return "/job_queue/pop";
    }
}
