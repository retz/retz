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
package io.github.retz.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import javax.security.cert.X509Certificate;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.atomic.AtomicInteger;

// DANGER ZONE: this disables TLS certification and allow self-signed certificate
// do not use this over internet
// Copyright notice: this function is based on StackOverflow:19540289
public class WrongTrustManager implements X509TrustManager {
    static final Logger LOG = LoggerFactory.getLogger(Client.class);
    static final HostnameVerifier originalHostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();
    static final SSLSocketFactory originalSSLSocketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
    static final AtomicInteger ref = new AtomicInteger(0);

    static synchronized void disableTLS() throws NoSuchAlgorithmException, KeyManagementException {
        if (ref.getAndIncrement() > 0) { // somebody's already using
            return;
        }

        LOG.warn("DANGER ZONE: TLS certificate check is disabled. Set 'retz.tls.insecure = false' at config file to supress this message.");
        TrustManager[] trustAllCerts = new TrustManager[]{new WrongTrustManager()};

        HttpsURLConnection.getDefaultHostnameVerifier();
        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HostnameVerifier allHostsValid = new HostnameVerifier() {
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        };
        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    }

    static synchronized void enableTLS() {
        if (ref.decrementAndGet() > 0) { // somebody's still using
            return;
        } else if (ref.get() < 0) {
            ref.set(0);
        }

        HttpsURLConnection.setDefaultHostnameVerifier(originalHostnameVerifier);
        HttpsURLConnection.setDefaultSSLSocketFactory(originalSSLSocketFactory);
    }


    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
        return null;
    }

    public void checkClientTrusted(X509Certificate[] certs, String authType) {
    }

    public void checkServerTrusted(X509Certificate[] certs, String authType) {
    }

    @Override
    public void checkClientTrusted(java.security.cert.X509Certificate[] paramArrayOfX509Certificate, String paramString)
            throws CertificateException {
        // TODO Auto-generated method stub

    }

    @Override
    public void checkServerTrusted(java.security.cert.X509Certificate[] paramArrayOfX509Certificate, String paramString)
            throws CertificateException {
        // TODO Auto-generated method stub

    }

}
