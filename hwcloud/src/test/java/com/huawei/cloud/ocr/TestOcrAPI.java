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

package com.huawei.cloud.ocr;

import java.io.File;
import java.io.IOException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.huawei.cloud.orc.HWOcrClientAKSK;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.junit.Test;

/**
 * TestOcrAPI
 */
public class TestOcrAPI {

  String path;

  {
    try {
      path = new File("../cloud/src/test/resources/ocr").getCanonicalPath();
    } catch (IOException e) {
    }
  }

  @Test
  public void testOcrIdCard() throws IOException {
    String Httpendpoint="https://ocr.cn-north-1.myhuaweicloud.com"; //httpendpoint for the service
    String Region="cn-north-1";   //region name for the service
    String AK="provide your AK";  //AK from authentication
    String SK="provide your SK"; //SK from authentication
    try {
      HWOcrClientAKSK ocrClient=new HWOcrClientAKSK(Httpendpoint,Region,AK,SK);
      HttpResponse
          response=ocrClient.requestOcrServiceBase64("/v1.0/ocr/id-card", path + "/id-card-demo.png");
      System.out.println(response);
      String content = IOUtils.toString(response.getEntity().getContent());
      // System.out.println(content);

      JsonParser parser = new JsonParser();
      JsonObject jsonObject = parser.parse(content).getAsJsonObject();
      JsonObject result = jsonObject.getAsJsonObject("result");
      // System.out.println(result.toString());
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

}
