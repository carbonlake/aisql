/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.cloud.credential;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import com.google.gson.Gson;
import com.huawei.cloud.RestConstants;
import com.huawei.cloud.util.RestUtil;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;


/**
 * Helps to login to Huawei cloud.
 */
public class LoginRequestManager implements Serializable {

  public static LoginInfo login(String username, String password, OkHttpClient client) {
    String loginJson = "{\n" +
        "  \"auth\": {\n" +
        "    \"identity\": {\n" +
        "      \"methods\": [\"password\"],\n" +
        "      \"password\": {\n" +
        "        \"user\": {\n" +
        "          \"name\": \"" + username + "\",\n" +
        "          \"password\": \"" + password + "\",\n" +
        "          \"domain\": {\n" +
        "            \"name\": \"" + username + "\"\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    },\n" +
        "    \"scope\": {\n" +
        "      \"project\": {\n" +
        "        \"name\": \"cn-north-1\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}";
    LoginInfo loginInfo = new LoginInfo();
    Exception[] exception = new Exception[1];
    RestUtil.postAsync(RestConstants.HUAWEI_CLOUD_AUTH_ENDPOINT, loginJson,
        new Callback() {
          @Override public void onFailure(Call call, IOException e) {
            exception[0] = e;
          }

          @Override public void onResponse(Call call, Response response) throws IOException {
            if (response.isSuccessful()) {
              ObjectMapper objectMapper = new ObjectMapper();
              Map<String, Object> jsonNodeMap = null;
              try {
                jsonNodeMap = objectMapper
                    .readValue(response.body().string(), new TypeReference<Map<String, Object>>() {
                    });
              } catch (Exception e) {
                exception[0] = e;
              }
              loginInfo.setUserName(username);
              loginInfo.setLoggedIn(true);
              loginInfo.setToken(response.header(RestConstants.AUTH_TOKEN_HEADER));
              loginInfo.setProjectId(
                  ((Map) ((Map) jsonNodeMap.get("token")).get("project")).get("id").toString());
            }
          }
        }, client);
    while (exception[0] == null && !loginInfo.isLoggedIn()) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    if (exception[0] != null) {
      throw new RuntimeException(exception[0]);
    }
    return loginInfo;
  }

  public static Credential getTemporaryAccessKeys(LoginInfo loginInfo, OkHttpClient client)
      throws IOException {
    String accessJson = "{\n" +
        "    \"auth\": {\n" +
        "        \"identity\": {\n" +
        "            \"methods\": [\n" +
        "                \"token\"\n" +
        "            ],\n" +
        "            \"token\": {\n" +
        "                \"id\": \"" + loginInfo.getToken() + "\",\n" +
        "                \"duration-seconds\": \"3600\"\n" +
        "\n" +
        "            }\n" +
        "        }\n" +
        "    }\n" +
        "}";

    Response response =
        RestUtil.postSync(RestConstants.HUAWEI_CLOUD_SECURITYTOKEN_ENDPOINT, accessJson, client);
    Gson gson = new Gson();
    if (response.isSuccessful()) {
      AccessInfo accessInfo = gson.fromJson(response.body().string(), AccessInfo.class);
      return accessInfo.getCredential();
    } else {
      throw new IOException(response.body().string());
    }
  }

  public static class LoginInfo {
    private boolean loggedIn = false;
    private String token;
    private String projectId;
    private String userName;

    public String getToken() {
      return token;
    }

    public void setToken(String token) {
      this.token = token;
    }

    public String getProjectId() {
      return projectId;
    }

    public void setProjectId(String projectId) {
      this.projectId = projectId;
    }

    public String getUserName() {
      return userName;
    }

    public void setUserName(String userName) {
      this.userName = userName;
    }

    public boolean isLoggedIn() {
      return loggedIn;
    }

    public void setLoggedIn(boolean loggedIn) {
      this.loggedIn = loggedIn;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LoginInfo loginInfo = (LoginInfo) o;
      return Objects.equals(userName, loginInfo.userName);
    }

    @Override public int hashCode() {

      return Objects.hash(userName);
    }
  }

  public static class AccessInfo implements Serializable {

    private Credential credential;

    public Credential getCredential() {
      return credential;
    }

    public void setCredential(Credential credential) {
      this.credential = credential;
    }
  }

  public static class Credential implements Serializable {

    private String access;

    private String secret;

    private String expires_at;

    private String securitytoken;

    public String getAccess() {
      return access;
    }

    public void setAccess(String access) {
      this.access = access;
    }

    public String getSecret() {
      return secret;
    }

    public void setSecret(String secret) {
      this.secret = secret;
    }

    public String getExpires_at() {
      return expires_at;
    }

    public void setExpires_at(String expires_at) {
      this.expires_at = expires_at;
    }

    public String getSecuritytoken() {
      return securitytoken;
    }

    public void setSecuritytoken(String securitytoken) {
      this.securitytoken = securitytoken;
    }
  }
}
