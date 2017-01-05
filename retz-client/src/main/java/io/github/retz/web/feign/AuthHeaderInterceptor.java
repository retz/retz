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
package io.github.retz.web.feign;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import io.github.retz.auth.AuthHeader;
import io.github.retz.auth.Authenticator;
import io.github.retz.cli.TimestampHelper;

public class AuthHeaderInterceptor implements RequestInterceptor {

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

    @Override
    public void apply(RequestTemplate template) {
        String md5 = "";
        byte[] body = template.body();
        if (body != null) {
            md5 = Base64.getEncoder().withoutPadding().encodeToString(DIGEST.get().digest(body));
            template.header("Content-MD5", md5);
        }

        String date = TimestampHelper.now();
        template.header("Date", date);

        String resource;
        try {
            resource = new URI(template.url()).getPath() + template.queryLine();
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }

        String header = authenticator.header(template.method(), md5, date, resource).buildHeader();
        template.header(AuthHeader.AUTHORIZATION, header);
    }
}
