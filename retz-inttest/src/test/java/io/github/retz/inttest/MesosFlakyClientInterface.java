/**
 *    Retz
 *    Copyright (C) 2016-2017 Nautilus Technologies, Inc.
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
package io.github.retz.inttest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import feign.*;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.slf4j.Slf4jLogger;
import io.github.retz.web.NoOpHostnameVerifier;
import io.github.retz.web.WrongTrustManager;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public interface MesosFlakyClientInterface {
    @RequestLine("GET /tasks?limit={limit}&offset={offset}&order={order}")
    @Headers({"Content-Type: application/json", "Accept: application/json"})
    Map<String, Object> tasks(@Param("limit") int limit,
            @Param("offset") long offset,
            @Param("order") String order);

    static MesosFlakyClientInterface connect(
            URI uri) {
        String url = Objects.requireNonNull(uri, "uri cannot be null").toString();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        SSLSocketFactory socketFactory;
        HostnameVerifier hostnameVerifier;
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, new TrustManager[]{new WrongTrustManager()}, new java.security.SecureRandom());
            socketFactory = sc.getSocketFactory();
            hostnameVerifier = new NoOpHostnameVerifier();

            return Feign.builder()
                    .client(new Client.Default(socketFactory, hostnameVerifier))
                    .logger(new Slf4jLogger())
                    .encoder(new JacksonEncoder(mapper))
                    .decoder(new JacksonDecoder(mapper))
                    .target(MesosFlakyClientInterface.class, url);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e.toString());
        } catch (KeyManagementException e) {
            throw new AssertionError(e.toString());
        }


    }

    static Response tryOrErrorResponse(Supplier<Response> supplier) throws IOException {
        try {
            return supplier.get();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }
}
