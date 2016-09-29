package io.github.retz.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.retz.protocol.data.Application;

public class LoadAppFromMasterRequest extends Request {
    private Application application;

    @JsonCreator
    public LoadAppFromMasterRequest(@JsonProperty(value = "application", required = true) Application app) {
        this.application = app;
    }

    @JsonGetter("application")
    public Application application() {
        return application;
    }

    @Override
    public String resource() {
        return "/replica/app/" + application.getAppid();
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
        return "/replica/app/:name";
    }
}
