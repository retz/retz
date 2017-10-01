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
package io.github.retz.grpc;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import io.github.retz.auth.AuthHeader;
import io.github.retz.auth.Authenticator;
import io.github.retz.cli.TimestampHelper;
import io.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class AuthHeaderInterceptor implements ClientInterceptor {
    static final Logger LOG = LoggerFactory.getLogger(AuthHeaderInterceptor.class);

    static final Metadata.Key<String> AUTH_HEADER_KEY =
            Metadata.Key.of(AuthHeader.AUTHORIZATION, Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> DATE_HEADER_KEY =
            Metadata.Key.of("Date", Metadata.ASCII_STRING_MARSHALLER);

    private static final ThreadLocal<MessageDigest> DIGEST = ThreadLocal
            .withInitial(() -> {
                try {
                    return MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new AssertionError("No such algorithm callled MD5.", e);
                }
            });

    private final Authenticator authenticator;

    public AuthHeaderInterceptor(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    private AuthHeaderInterceptor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                String date = TimestampHelper.now();
                AuthHeader authHeader = authenticator.header(method.getType().name(), "md5", date , method.getFullMethodName());
                headers.put(AUTH_HEADER_KEY, authHeader.buildHeader());
                headers.put(DATE_HEADER_KEY, date);
                LOG.info("Signature: {}", authHeader.buildHeader());
                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                    @Override
                    public void onHeaders(Metadata headers) {
                        /**
                         * if you don't need receive header from server,
                         * you can use {@link io.grpc.stub.MetadataUtils#attachHeaders}
                         * directly to send header
                         */
                        super.onHeaders(headers);
                    }
                }, headers);
            }
        };
    }
}
