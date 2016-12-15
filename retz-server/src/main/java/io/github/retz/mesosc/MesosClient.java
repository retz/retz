package io.github.retz.mesosc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;

import java.net.URI;
import java.util.Objects;

public class MesosClient {
    public static MesosMaster connect(URI uri) {
        String url = Objects.requireNonNull(uri, "uri cannot be null").toString();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        return Feign.builder()
                .encoder(new JacksonEncoder(mapper))
                .decoder(new JacksonDecoder(mapper))
                //.errorDecoder(new ErrorResponseDecoder(mapper))
                //.requestInterceptor(new AuthHeaderInterceptor(authenticator))
                .target(MesosMaster.class, url);
    }
}
