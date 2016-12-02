/**
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package io.github.retz.web.feign;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import feign.Feign;
import feign.Param;
import feign.RequestLine;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import io.github.retz.auth.Authenticator;
import io.github.retz.protocol.LoadAppRequest;
import io.github.retz.protocol.Response;
import io.github.retz.protocol.ScheduleRequest;
import io.github.retz.protocol.data.Application;
import io.github.retz.protocol.data.Job;

public interface Server {

    @RequestLine("GET /ping")
    String ping();

    @RequestLine("GET /jobs")
    Response list();

    default Response schedule(Job job) {
        return schedule(new ScheduleRequest(job));
    }

    @RequestLine("POST /job")
    Response schedule(ScheduleRequest request);

    @RequestLine("GET /job/{id}")
    Response getJob(@Param("id") int id);

    @RequestLine("GET /job/{id}/file?path={path}&offset={offset}&length={length}")
    Response getFile(
            @Param("id") int id,
            @Param("path") String path,
            @Param("offset") long offset,
            @Param("length") long length);

    @RequestLine("GET /job/{id}/dir?path={path}")
    Response listFiles(@Param("id") int id, @Param("path") String path);

    default Response run(Job job) {
        return run(new ScheduleRequest(job));
    }

    @RequestLine("POST /job")
    Response run(ScheduleRequest request);

    @RequestLine("DELETE /job/{id}")
    Response kill(@Param("id") int id);

    @RequestLine("GET /app/{appid}")
    Response getApp(@Param("appid") String appid);

    default Response load(Application application) {
        return load(application.getAppid(), new LoadAppRequest(application));
    }

    @RequestLine("PUT /app/{appid}")
    Response load(@Param("appid") String appid, LoadAppRequest request);

    @RequestLine("GET /apps")
    Response listApp();

    @Deprecated
    @RequestLine("DELETE /app/{appid}")
    Response unload(@Param("appid") String appid);

    static Server connect(URI uri, Authenticator authenticator) {
        String url = Objects.requireNonNull(uri, "uri cannot be null").toString();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        return Feign.builder()
                .encoder(new JacksonEncoder(mapper))
                .decoder(new JacksonDecoder(mapper))
                .errorDecoder(new ErrorResponseDecoder(mapper))
                .requestInterceptor(new AuthHeaderInterceptor(authenticator))
                .target(Server.class, url);
    }

    static Response tryOrErrorResponse(Supplier<Response> supplier) throws IOException {
        try {
            return supplier.get();
        } catch (ErrorResponseException ex) {
            return ex.getErr();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }
}
