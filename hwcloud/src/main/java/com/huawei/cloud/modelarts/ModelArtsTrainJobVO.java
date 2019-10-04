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

package com.huawei.cloud.modelarts;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import org.apache.spark.sql.aisql.intf.DataScan;

/**
 * It the VO object which can holds all necessary information to generate training job json string.
 */
public class ModelArtsTrainJobVO implements Serializable {

  private static final long serialVersionUID = 2039946030391082620L;

  private String job_name;

  private String job_desc = "";

  private Config config;

  public String getJob_name() {
    return job_name;
  }

  public void setJob_name(String job_name) {
    this.job_name = job_name;
  }

  public String getJob_desc() {
    return job_desc;
  }

  public void setJob_desc(String job_desc) {
    this.job_desc = job_desc;
  }

  public Config getConfig() {
    return config;
  }

  public void setConfig(Config config) {
    this.config = config;
  }

  public static class Config implements Serializable {

    private static final long serialVersionUID = 3335320401332250349L;

    private Integer worker_server_num;

    private List<Map<String, String>> parameter;

    private String train_url;

    private Long engine_id;

    private Long spec_id;

    private Long model_id;

    private String user_image_url;

    private String user_command;

    private String data_url;

    private String app_url;

    private String boot_file_url;

    private String log_url;

    private String dataset_id;

    private String dataset_version_id;

    private String dataset_name;

    private String dataset_version_name;

    public Integer getWorker_server_num() {
      return worker_server_num;
    }

    public void setWorker_server_num(Integer worker_server_num) {
      this.worker_server_num = worker_server_num;
    }

    public List<Map<String, String>> getParameter() {
      return parameter;
    }

    public void setParameter(List<Map<String, String>> parameter) {
      this.parameter = parameter;
    }

    public void setParameterKV(String key, String value) {
      if (parameter == null) {
        parameter = new ArrayList<>();
      }
      Map<String, String> map = new HashMap<>();
      map.put("label", key);
      map.put("value", value);
      parameter.add(map);
    }

    public String getTrain_url() {
      return train_url;
    }

    public void setTrain_url(String train_url) {
      this.train_url = train_url;
    }

    public Long getEngine_id() {
      return engine_id;
    }

    public void setEngine_id(Long engine_id) {
      this.engine_id = engine_id;
    }

    public String getApp_url() {
      return app_url;
    }

    public void setApp_url(String app_url) {
      this.app_url = app_url;
    }

    public String getBoot_file_url() {
      return boot_file_url;
    }

    public void setBoot_file_url(String boot_file_url) {
      this.boot_file_url = boot_file_url;
    }

    public String getLog_url() {
      return log_url;
    }

    public void setLog_url(String log_url) {
      this.log_url = log_url;
    }

    public Long getSpec_id() {
      return spec_id;
    }

    public void setSpec_id(Long spec_id) {
      this.spec_id = spec_id;
    }

    public Long getModel_id() {
      return model_id;
    }

    public void setModel_id(Long model_id) {
      this.model_id = model_id;
    }

    public String getUser_image_url() {
      return user_image_url;
    }

    public void setUser_image_url(String user_image_url) {
      this.user_image_url = user_image_url;
    }

    public String getUser_command() {
      return user_command;
    }

    public void setUser_command(String user_command) {
      this.user_command = user_command;
    }

    public String getData_url() {
      return data_url;
    }

    public void setData_url(String data_url) {
      this.data_url = data_url;
    }

    public String getDataset_id() {
      return dataset_id;
    }

    public void setDataset_id(String dataset_id) {
      this.dataset_id = dataset_id;
    }

    public String getDataset_version_id() {
      return dataset_version_id;
    }

    public void setDataset_version_id(String dataset_version_id) {
      this.dataset_version_id = dataset_version_id;
    }

    public String getDataset_name() {
      return dataset_name;
    }

    public void setDataset_name(String dataset_name) {
      this.dataset_name = dataset_name;
    }

    public String getDataset_version_name() {
      return dataset_version_name;
    }

    public void setDataset_version_name(String dataset_version_name) {
      this.dataset_version_name = dataset_version_name;
    }
  }

  /**
   * Generates json for starting and creating training job.
   */
  static String generateJson(Map<String, String> options, String modelName, DataScan queryObject) {
    ModelArtsTrainJobVO modelVO = new ModelArtsTrainJobVO();
    modelVO.setJob_name(modelName);
    Config config = new Config();
    modelVO.setConfig(config);
    config.setApp_url(options.get("app_url"));
    config.setBoot_file_url(options.get("boot_file_url"));
    config.setData_url(options.get("data_url"));
    config.setUser_image_url(options.get("user_image_url"));
    config.setUser_command(options.get("user_command"));
    String engine_id = options.get("engine_id");
    if (engine_id != null) {
      config.setEngine_id(Long.parseLong(engine_id));
    }
    String spec_id = options.get("spec_id");
    if (spec_id != null) {
      config.setSpec_id(Long.parseLong(spec_id));
    }
    String model_id = options.get("model_id");
    if (model_id != null) {
      config.setModel_id(Long.parseLong(model_id));
    }
    config.setLog_url(options.get("log_url"));
    config.setTrain_url(options.get("train_url"));
    config.setDataset_id(options.get("dataset_id"));
    config.setDataset_name(options.get("dataset_name"));
    config.setDataset_version_name(options.get("dataset_version_name"));
    config.setDataset_version_id(options.get("dataset_version_id"));
    String serverNum = options.get("worker_server_num");
    if (serverNum != null) {
      config.setWorker_server_num(Integer.parseInt(serverNum));
    }
    String params = options.get("params");
    if (params != null) {
      String[] split = params.split(",");
      for (String s : split) {
        String[] kv = s.split("=");
        config.setParameterKV(kv[0], kv[1]);
      }
    }
    String projections =
        Arrays.asList(queryObject.getProjectionColumns()).stream().collect(Collectors.joining(","));
    config.setParameterKV("carbon_projections", projections);
    config.setParameterKV("carbon_table_path", queryObject.getTablePath());
    //TODO set the filters as well.
    Gson gson = new Gson();
    return gson.toJson(modelVO);
  }
}
