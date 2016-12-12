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
import java.util.Optional;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;

import feign.FeignException;
import feign.Response;
import feign.codec.ErrorDecoder;
import io.github.retz.auth.ProtocolTester;
import io.github.retz.protocol.ErrorResponse;
import io.github.retz.web.Client;
import io.github.retz.web.ClientHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorResponseDecoder implements ErrorDecoder {
    static final Logger LOG = LoggerFactory.getLogger(ErrorResponseDecoder.class);

    private final ObjectMapper mapper;

    private final ErrorDecoder delegate;

    private final Function<String, Optional<String>> extractor;

    public ErrorResponseDecoder(ObjectMapper mapper) {
        this(mapper, new ErrorDecoder.Default(), ErrorResponseDecoder::extract);
    }

    public ErrorResponseDecoder(
            ObjectMapper mapper,
            ErrorDecoder delegate,
            Function<String, Optional<String>> extractor) {
        this.mapper = mapper;
        this.delegate = delegate;
        this.extractor = extractor;
    }

    @Override
    public Exception decode(String methodKey, Response response) {
        Exception ex = delegate.decode(methodKey, response);

        String server = String.join(", ", response.headers().get("Server"));
        new ProtocolTester(server, Client.VERSION_STRING).test();

        if (ex instanceof ErrorResponseException) {
            return ex;
        } else if (ex instanceof FeignException) {

            FeignException fex = (FeignException) ex;
            return extractor.apply(fex.getMessage())
                    .map(value -> {
                        try {
                            return new ErrorResponseException(
                                    fex.status(), fex.getMessage(),
                                    mapper.readValue(value, ErrorResponse.class));
                        } catch (IOException e) {
                            return new ErrorResponseException(
                                    fex.status(), fex.getMessage(),
                                    new ErrorResponse(value));
                        }
                    })
                    .orElseGet(() -> new ErrorResponseException(
                            fex.status(), fex.getMessage(),
                            new ErrorResponse(fex.toString())));
        } else {
            return ex;
        }
    }

    private static final String SEPARATOR = "; content:\n";

    private static Optional<String> extract(String message) {
        int idx = message.indexOf(SEPARATOR);
        if (idx >= 0) {
            return Optional.of(message.substring(idx + SEPARATOR.length()));
        } else {
            return Optional.empty();
        }
    }
}
