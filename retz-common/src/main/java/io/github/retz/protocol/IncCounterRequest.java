package io.github.retz.protocol;

import java.util.Objects;

public class IncCounterRequest extends Request {

    public IncCounterRequest() {
    }

    @Override
    public String resource() {
        return "/counter/inc";
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
        return "/counter/inc";
    }
}
