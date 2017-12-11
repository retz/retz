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

import io.github.retz.auth.AuthHeader;
import io.github.retz.auth.Authenticator;
import io.github.retz.auth.HmacSHA256Authenticator;
import io.github.retz.db.Database;
import io.github.retz.protocol.data.User;
import io.grpc.*;
import io.netty.util.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class ServerAuthInterceptor implements ServerInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(ServerAuthInterceptor.class);

    static final Metadata.Key<String> AUTH_HEADER_KEY =
            Metadata.Key.of(AuthHeader.AUTHORIZATION, Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> DATE_HEADER_KEY =
            Metadata.Key.of("Date", Metadata.ASCII_STRING_MARSHALLER);

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        LOG.info("header: {} ? {} <<<", headers, headers.get(AUTH_HEADER_KEY));
        Optional<AuthHeader> maybeRemote = AuthHeader.parseHeaderValue(headers.get(AUTH_HEADER_KEY));
        AuthHeader remote = maybeRemote.get();
        LOG.info("key={}, date={}, signature={}, verb={}, resource={}", remote.key(), headers.get(DATE_HEADER_KEY), remote.signature(),
                call.getMethodDescriptor().getType().name(), call.getMethodDescriptor().getFullMethodName());
        try {
            Optional<User> user = Database.getInstance().getUser(remote.key());
            if (user.isPresent()) {
                Authenticator authenticator = new HmacSHA256Authenticator(user.get().keyId(), user.get().secret());

                String date = headers.get(DATE_HEADER_KEY);

                MethodDescriptor<ReqT, RespT> methodDescriptor = call.getMethodDescriptor();
                boolean result = authenticator.authenticate(methodDescriptor.getType().name(), "md5", date, methodDescriptor.getFullMethodName(),
                        authenticator.getKey(), remote.signature());

                // TODO: authenticate the client right here!!
                Context ctx = Context.current().withValue(RetzServer.USER_ID_KEY, remote.key());
                if (result) {
                    LOG.info("Authenticated! {}", remote.signature());
                    return Contexts.interceptCall(ctx, call, headers, next);
                }
            }
            call.close(Status.UNAUTHENTICATED, new Metadata());

        } catch (IOException e) {
            call.close(Status.UNAVAILABLE, new Metadata());
        }
        return new ServerCall.Listener<ReqT>() {};
    }
}
