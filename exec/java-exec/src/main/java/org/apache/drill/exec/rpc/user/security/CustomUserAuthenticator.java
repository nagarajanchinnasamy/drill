/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.user.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * Implement {@link org.apache.drill.exec.rpc.user.security.UserAuthenticator} to connect to obtain a session id
 * from an external system and set it as a session option for further use by UDFs
 */
@UserAuthenticatorTemplate(type = "customAuthenticator")
public class CustomUserAuthenticator implements UserAuthenticator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomUserAuthenticator.class);

  private static final String CUSTOM_AUTHENTICATOR_PROTOCOL = "drill.exec.security.user.auth.custom_authenticator_protocol";
  private static final String CUSTOM_AUTHENTICATOR_URL = "drill.exec.security.user.auth.custom_authenticator_url";
  private static final String CUSTOM_AUTHENTICATOR_USER_HEADER = "drill.exec.security.user.auth.custom_authenticator_user_header";
  private static final String CUSTOM_AUTHENTICATOR_PASSWORD_HEADER = "drill.exec.security.user.auth.custom_authenticator_password_header";
  private static final String CUSTOM_AUTHENTICATOR_SESSION_ID_HEADER = "drill.exec.security.user.auth.custom_authenticator_session_id_header";
  private static final String CUSTOM_AUTHENTICATOR_SESSION_ID = "drill.exec.security.user.auth.custom_authenticator_session_id";

  private String protocol;
  private String url;
  private String userHeader;
  private String passwordHeader;
  private String sessionIdHeader;
  private String sessionId;

  @Override
  public void setup(DrillConfig drillConfig) throws DrillbitStartupException {
  protocol = drillConfig.getString(CUSTOM_AUTHENTICATOR_PROTOCOL);
    url = drillConfig.getString(CUSTOM_AUTHENTICATOR_URL);
    userHeader = drillConfig.getString(CUSTOM_AUTHENTICATOR_USER_HEADER);
    passwordHeader = drillConfig.getString(CUSTOM_AUTHENTICATOR_PASSWORD_HEADER);
    sessionIdHeader = drillConfig.getString(CUSTOM_AUTHENTICATOR_SESSION_ID_HEADER);

    try {
      new HttpClient();
    } catch(LinkageError e) {
      final String errMsg = "Problem in finding the native library of HttpClient (Apache HTTP Components - HttpClient API). " +
          "Make sure to set Drillbit JVM option 'java.library.path' to point to the directory where the native " +
          "HttpClient exists.";
      logger.error(errMsg, e);
      throw new DrillbitStartupException(errMsg + ":" + e.getMessage(), e);
    }
  }

  @Override
  public List<OptionValue> authenticate(String user, String password) throws UserAuthenticationException {
    DefaultHttpClient httpClient = new DefaultHttpClient();
    HttpPost httpPost = new HttpPost(protocol + url);
    httpPost.setHeader(userHeader, user);
    httpPost.setHeader(passwordHeader, password);
    HttpResponse response = null;

    try {
      response = httpClient.execute(httpPost);
      sessionId = response.getLastHeader(sessionIdHeader).getValue();
    } catch(Exception ex) {
        throw new UserAuthenticationException(String.format("Authentication of '%s' failed.", user));
    }

    if (null == sessionId) {
      return null;
    }

    List<OptionValue> options = new ArrayList<OptionValue>();
    options.add(OptionValue.createString(OptionType.SESSION, CUSTOM_AUTHENTICATOR_SESSION_ID, sessionId));
    return options;
  }

  @Override
  public void close() throws IOException {
    // No-op as no resources are occupied by this authenticator.
  }
}
