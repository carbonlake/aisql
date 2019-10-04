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

import com.google.gson.JsonObject;
import com.huawei.ais.common.AuthInfo;
import com.huawei.ais.common.ProxyHostInfo;
import com.huawei.ais.sdk.AisAccess;
import com.huawei.ais.sdk.AisAccessWithProxy;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.entity.StringEntity;

/**
 * copy from ocr sdk
 */
public class HWOcrClientAKSK {
  private AuthInfo HEC_AUTH;       //Authorization Information
  private AisAccess HttpService;   //access to ocr service on cloud
  private ProxyHostInfo PROXYINFO; //proxy host info (optional)

  /**
   * Constructor for OcrClientAKSK without proxy
   * @param Endpoint HTTPEndPoint for OCR Service
   * @param Region The region info for OCR Service
   * @param YourAK The Access Key from Authentication
   * @param YourSK The Secret Key from Authentication
   * */
  public HWOcrClientAKSK(String Endpoint, String Region, String YourAK, String YourSK) {
    if (Endpoint == null | Endpoint == "" | Region == null | Region == "" | YourAK == null
        | YourAK == "" | YourSK == null | YourSK == "") throw new IllegalArgumentException(
        "the parameter for HWOcrClientAKSK's Constructor cannot be empty");
    HEC_AUTH = new AuthInfo(Endpoint, Region, YourAK, YourSK);
    HttpService = new AisAccess(HEC_AUTH);
  }

  /**
   * Constructor for OcrClientAKSK with proxy
   * @param Endpoint HTTPEndPoint for OCR Service
   * @param Region The region info for OCR Service
   * @param YourAK The Access Key from Authentication
   * @param YourSK The Secret Key from Authentication
   * @param ProxyHost The URL for the proxy server
   * @param ProxyPort The Port number for the proxy service
   * @param ProxyUsrName The Loggin user for the proxy server if authentication is required
   * @param ProxyPwd  The Loggin pwd for the proxy server if authentication is required
   * */
  public HWOcrClientAKSK(String Endpoint, String Region, String YourAK, String YourSK,
      String ProxyHost, int ProxyPort, String ProxyUsrName, String ProxyPwd) {
    if (Endpoint == null | Endpoint == "" | Region == null | Region == "" | YourAK == null
        | YourAK == "" | YourSK == null | YourSK == "" | ProxyHost == null | ProxyHost == ""
        | ProxyUsrName == null | ProxyUsrName == "" | ProxyPwd == null | ProxyPwd == "")
      throw new IllegalArgumentException(
          "the parameter for HWOcrClientAKSK's Constructor cannot be empty");
    HEC_AUTH = new AuthInfo(Endpoint, Region, YourAK, YourSK);
    PROXYINFO = new ProxyHostInfo(ProxyHost, ProxyPort, ProxyUsrName, ProxyPwd);
    HttpService = new AisAccessWithProxy(HEC_AUTH, PROXYINFO);
  }

  /*
   * Release the access service when the object is collected
   * */
  protected void finalize() {
    HttpService.close();
  }

  /**
   * Call the OCR API with a local picture file
   * @param uri the uri for the http request to be called
   * @param filepath the path for the picture file to be recognized
   * */
  public HttpResponse requestOcrServiceBase64(String uri, String filepath) throws IOException {
    if (uri == null | uri == "" | filepath == null | filepath == "")
      throw new IllegalArgumentException(
          "the parameter for requestOcrServiceBase64 cannot be empty");
    try {

      byte[] fileData = FileUtils.readFileToByteArray(new File(filepath));
      String fileBase64Str = Base64.encodeBase64String(fileData);

      JsonObject gson = new JsonObject();
      gson.addProperty("image", fileBase64Str);

      //
      // 2.a (Optional) Specify whether the information is obtained from the front or back side of the ID card.
      // side Possible value: front, back, or unspecified. If the parameter is left unspecified or the parameter is not included, the algorithm automatically determine the side from which the information is obtained. You are advised to specify this parameter to improve the accuracy.
      //
      //json.put("side", "front");

      StringEntity stringEntity = new StringEntity(gson.toString(), "utf-8");

      // 3. Pass the URI and required parameters of ID Card OCR.
      // Pass the parameters in JSON objects and invoke the service using POST.
      HttpResponse response = HttpService.post(uri, stringEntity);
      return response;
      // 4. Check whether the service invocation is successful. If 200 is returned, the invocation succeeds. Otherwise, it fails.
      //ResponseProcessUtils.processResponseStatus(response);

      // 5. Process the character stream returned by the service.
      //ResponseProcessUtils.processResponse(response);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

}
