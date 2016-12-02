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
import io.github.retz.protocol.ErrorResponse;

public class ErrorResponseDecoder implements ErrorDecoder {

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
                            return ex;
                        }
                    })
                    .orElseGet(() -> ex);
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
