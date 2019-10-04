/**
 * Copyright 2018 Huawei Technologies Co.,Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 **/
package com.huawei.cloud.orc;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.huawei.ais.sdk.util.HttpClientUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;

/**
 * copy from ocr sdk
 */
public class HWOcrClientToken {
  private String domainName;
      //domainName the same with username for common user.different for iam user
  private String username;    //username of the user
  private String passwd;      //password of the user
  private String regionName;  //region for ocr service(cn-north-1,cn-south-1 etc.)
  private String token;       //A serial of character returned by iam server for authentication
  private String httpEndpoint; //Http endpoint for the service
  private static final Integer RETRY_MAX_TIMES = 3; // 获取token的最大重试次数
  private static final long POLLING_INTERVAL = 2000L; // 两次获取token间隔时间

  /**
   *
   * @param domain  domainName for iam user.If it's common user ,domainName is the same with usrname
   * @param usrname  username of the user
   * @param password password of the user
   * @param region   region for ocr service(cn-north-1,cn-south-1 etc.)
   * @param endpoint http endpoint for the url
   */
  public HWOcrClientToken(String domain, String usrname, String password, String region,
      String endpoint) {
    if (domain == null | domain == "" | usrname == null | usrname == "" | password == null
        | password == "" | region == null | region == "" | endpoint == null | endpoint == "")
      throw new IllegalArgumentException(
          "the parameter for HWOcrClientToken's Constructor cannot be empty");
    domainName = domain;
    username = usrname;
    passwd = password;
    regionName = region;
    token = "";
    httpEndpoint = endpoint;

  }

  /**
   * Construct a token request object for accessing the service using token authentication.
   * For details, see the following document:
   * https://support.huaweicloud.com/en-us/api-ocr/ocr_03_0005.html
   *
   * @return Construct the JSON object of the access.
   */
  private String requestBody() {
    JsonObject auth = new JsonObject();

    JsonObject identity = new JsonObject();

    JsonArray methods = new JsonArray();
    methods.add (new JsonPrimitive("password"));
    identity.add("methods", methods);

    JsonObject password = new JsonObject();

    JsonObject user = new JsonObject();
    user.addProperty("name", username);
    user.addProperty("password", passwd);

    JsonObject domain = new JsonObject();
    domain.addProperty("name", domainName);
    user.add("domain", domain);

    password.add("user", user);

    identity.add("password", password);

    JsonObject scope = new JsonObject();

    JsonObject scopeProject = new JsonObject();
    scopeProject.addProperty("name", regionName);

    scope.add("project", scopeProject);

    auth.add("identity", identity);
    auth.add("scope", scope);

    JsonObject params = new JsonObject();
    params.add("auth", auth);
    return params.toString();
  }

  private void refreshToken() throws InterruptedException, IOException, URISyntaxException {
    System.out.println("Token expired, refresh.");
    token = "";
    getToken();
  }

  /**
   * Obtain the token parameter. Note that this function aims to extract the token from the header in the HTTP request response body.
   * The parameter name is X-Subject-Token.
   *
   * @throws URISyntaxException
   * @throws UnsupportedOperationException
   * @throws IOException
   */
  private void getToken()
      throws URISyntaxException, UnsupportedOperationException, IOException, InterruptedException {
    //if token is not empty return token directly
    if (0 != token.length()) return;

    // 获取token尝试次数
    Integer retryTimes = 0;

    // 1.构建获取Token所需要的参数
    String requestBody = requestBody();
    int iamSuccessResponseCode = 201;
    String url = String.format("https://iam.%s.myhuaweicloud.com/v3/auth/tokens", regionName);
    Header[] headers =
        new Header[] { new BasicHeader("Content-Type", ContentType.APPLICATION_JSON.toString()) };
    StringEntity stringEntity = new StringEntity(requestBody, "utf-8");

    while (true) {
      // 2.传入IAM服务对应的参数, 使用POST方法调用服务并解析出Token value
      HttpResponse response = HttpClientUtils.post(url, headers, stringEntity);
      //判断token获取http状态码，获取成功后才取出token，失败返回null
      if (iamSuccessResponseCode != response.getStatusLine().getStatusCode()) {
        if (retryTimes < RETRY_MAX_TIMES) {
          retryTimes++;
          String content = IOUtils.toString(response.getEntity().getContent());
          System.out.println(content);
          token = "";
          Thread.sleep(POLLING_INTERVAL);    //延时2秒
          continue;
        } else {
          token = "";
          System.out.println("Get Token Failed!");
          return;
        }
      } else {
        System.out.println("Get Token Success!");
        Header[] xst = response.getHeaders("X-Subject-Token");
        token = xst[0].getValue();
        return;
      }
    }
  }

  /**
   * Use the Base64-encoded file and access VAT Invoice OCR using token authentication.
   * @throws URISyntaxException
   * @throws UnsupportedOperationException
   * @throws IOException
   */
  public HttpResponse requestOcrServiceBase64(String uri, String filepath)
      throws UnsupportedOperationException, URISyntaxException, IOException, InterruptedException {
    int iamTokenAbnormalCode = 403;
    int iamTokenExpiredCode = 401;
    String iamTokenAbnormalInfo = "The authentication token is abnormal.";
    String iamTokenExpiredInfo = "Token expired.";

    // 1. Construct the parameters required for accessing VAT Invoice OCR.
    if (uri == null | uri == "" | filepath == null | filepath == "")
      throw new IllegalArgumentException(
          "the parameter for requestOcrServiceBase64 cannot be empty");
    String completeurl = "https://" + httpEndpoint + uri;
    getToken();

    if (token != "") {
      Header[] headers = new Header[] { new BasicHeader("X-Auth-Token", token),
          new BasicHeader("Content-Type", ContentType.APPLICATION_JSON.toString()) };
      try {
        byte[] fileData = FileUtils.readFileToByteArray(new File(filepath));
        String fileBase64Str = Base64.encodeBase64String(fileData);
        JsonObject json = new JsonObject();
        json.addProperty("image", fileBase64Str);
        StringEntity stringEntity = new StringEntity(json.toString(), "utf-8");

        // 2. Pass the parameters of VAT Invoice OCR, invoke the service using the POST method, and parse and output the recognition result.
        HttpResponse response = HttpClientUtils.post(completeurl, headers, stringEntity);
        String content = IOUtils.toString(response.getEntity().getContent());

        if ((iamTokenAbnormalCode == response.getStatusLine().getStatusCode()) && (content
            .contains(iamTokenAbnormalInfo))
            || (iamTokenExpiredCode == response.getStatusLine().getStatusCode()) && (content
            .contains(iamTokenExpiredInfo))) {
          //	token expired,refresh token
          refreshToken();
          return requestOcrServiceBase64(uri, filepath);
        }
        return response;
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    }
    return null;
  }

}
