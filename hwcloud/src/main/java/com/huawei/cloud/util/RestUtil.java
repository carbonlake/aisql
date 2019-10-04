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

package com.huawei.cloud.util;

import java.io.IOException;

import okhttp3.*;

public class RestUtil {

  private static MediaType JSON = MediaType.get("application/json; charset=utf-8");
  public static MediaType BINARY = MediaType.get("multipart/form-data; boundary=xxBOUNDARYxx");

  public static void postAsync(String url, String json, Callback callback, String token,
      OkHttpClient client) {
    RequestBody body = RequestBody.create(JSON, json);
    Request.Builder builder = new Request.Builder().url(url).post(body);
    if (token != null) {
      builder = builder.addHeader("X-Auth-Token", token);
    }
    final Request request = builder.build();
    Call call = client.newCall(request);
    call.enqueue(callback);
  }

  public static Response postSync(String url, String json, String token,
      OkHttpClient client) throws IOException {
    RequestBody body = RequestBody.create(JSON, json);
    Request.Builder builder = new Request.Builder().url(url).post(body);
    if (token != null) {
      builder = builder.addHeader("X-Auth-Token", token);
    }
    final Request request = builder.build();
    Call call = client.newCall(request);
    return call.execute();
  }

  public static Response postSync(String url, RequestBody body, String token,
      OkHttpClient client) throws IOException {
    Request.Builder builder = new Request.Builder().url(url).post(body);
    if (token != null) {
      builder = builder.addHeader("X-Auth-Token", token);
    }
    final Request request = builder.build();
    Call call = client.newCall(request);
    return call.execute();
  }

  public static void postAsync(String url, String json, Callback callback, OkHttpClient client) {
    postAsync(url, json, callback, null, client);
  }

  public static Response get(String url, String token, OkHttpClient client)
      throws IOException {
    Request.Builder builder = new Request.Builder().url(url).get();
    builder = builder.addHeader("X-Auth-Token", token);
    final Request request = builder.build();
    Call call = client.newCall(request);
    return call.execute();
  }

  public static Response postSync(String url, String json, OkHttpClient client)
      throws IOException {
    RequestBody body = RequestBody.create(JSON, json);
    Request.Builder builder = new Request.Builder().url(url).post(body);
    final Request request = builder.build();
    Call call = client.newCall(request);
    return call.execute();
  }

  public static Response delete(String url, String token, OkHttpClient client)
      throws IOException {
    Request.Builder builder = new Request.Builder().url(url).delete();
    builder = builder.addHeader("X-Auth-Token", token);
    final Request request = builder.build();
    Call call = client.newCall(request);
    return call.execute();
  }
}
