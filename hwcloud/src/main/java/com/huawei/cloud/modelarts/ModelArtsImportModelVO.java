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
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.huawei.cloud.RestConstants;

/**
 * It the VO object which can holds all necessary information to import the model which is
 * generated by training job.
 */
public class ModelArtsImportModelVO implements Serializable {

  private static final long serialVersionUID = 9054691171703415400L;

  private String model_name;

  private String model_version;

  private String source_location;

  private String source_job_id = "";

  private String source_job_version = "";

  private String source_type;

  private String model_type;

  private String runtime;

  private String description = "";

  private String model_algorithm;

  private String execution_code;

  private List<Parameter> input_params;

  private List<Parameter> output_params;

  private List<Dependency> dependencies;

  private String initial_config;

  private List<CloudAPI> apis;

  private String model_metrics;

  private String workspace_id = "0";

  private String specification;

  public String getModel_name() {
    return model_name;
  }

  public void setModel_name(String model_name) {
    this.model_name = model_name;
  }

  public String getModel_version() {
    return model_version;
  }

  public void setModel_version(String model_version) {
    this.model_version = model_version;
  }

  public String getSource_location() {
    return source_location;
  }

  public void setSource_location(String source_location) {
    this.source_location = source_location;
  }

  public String getSource_job_id() {
    return source_job_id;
  }

  public void setSource_job_id(String source_job_id) {
    this.source_job_id = source_job_id;
  }

  public String getSource_job_version() {
    return source_job_version;
  }

  public void setSource_job_version(String source_job_version) {
    this.source_job_version = source_job_version;
  }

  public String getSource_type() {
    return source_type;
  }

  public void setSource_type(String source_type) {
    this.source_type = source_type;
  }

  public String getModel_type() {
    return model_type;
  }

  public void setModel_type(String model_type) {
    this.model_type = model_type;
  }

  public String getRuntime() {
    return runtime;
  }

  public void setRuntime(String runtime) {
    this.runtime = runtime;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getModel_algorithm() {
    return model_algorithm;
  }

  public void setModel_algorithm(String model_algorithm) {
    this.model_algorithm = model_algorithm;
  }

  public String getExecution_code() {
    return execution_code;
  }

  public void setExecution_code(String execution_code) {
    this.execution_code = execution_code;
  }

  public String getModel_metrics() {
    return model_metrics;
  }

  public void setModel_metrics(String model_metrics) {
    this.model_metrics = model_metrics;
  }

  public List<CloudAPI> getApis() {
    return apis;
  }

  public void setApis(List<CloudAPI> apis) {
    this.apis = apis;
  }

  public String getInitial_config() {
    return initial_config;
  }

  public void setInitial_config(String initial_config) {
    this.initial_config = initial_config;
  }

  public String getWorkspace_id() {
    return workspace_id;
  }

  public void setWorkspace_id(String workspace_id) {
    this.workspace_id = workspace_id;
  }

  public List<Dependency> getDependencies() {
    return dependencies;
  }

  public void setDependencies(List<Dependency> dependencies) {
    this.dependencies = dependencies;
  }

  public String getSpecification() {
    return specification;
  }

  public void setSpecification(String specification) {
    this.specification = specification;
  }

  public List<Parameter> getInput_params() {
    return input_params;
  }

  public void setInput_params(List<Parameter> input_params) {
    this.input_params = input_params;
  }

  public List<Parameter> getOutput_params() {
    return output_params;
  }

  public void setOutput_params(List<Parameter> output_params) {
    this.output_params = output_params;
  }

  public static String generateJSON(Map<String, String> options, String jobName, Config config,
      String executionCode) {
    Gson gson = new Gson();
    ModelArtsImportModelVO modelVO =
        getModelArtsImportModelVO(options, jobName, config, executionCode, gson);
    return gson.toJson(modelVO);
  }

  public static ModelArtsImportModelVO getModelArtsImportModelVO(Map<String, String> options,
      String jobName, Config config, String executionCode, Gson gson) {
    ModelArtsImportModelVO modelVO = new ModelArtsImportModelVO();
    modelVO.setModel_name(jobName);
    modelVO.setModel_version("0.0.1");
    String train_url = options.get("train_url");
    modelVO.setSource_location(getObsUrl(train_url));
    modelVO.setModel_type(config.getModel_type());
    modelVO.setApis(getUpdatedAPIS(config.getApis()));
    modelVO.setDependencies(config.getDependencies());
    modelVO.setExecution_code(getObsUrl(executionCode));
    modelVO.setInitial_config(gson.toJson(config));
    modelVO.setInput_params(getParameters(config, true));
    modelVO.setOutput_params(getParameters(config, false));
    modelVO.setModel_algorithm(config.getModel_algorithm());
    modelVO.setModel_metrics(gson.toJson(config.getMetrics()));
    return modelVO;
  }

  /**
   * Get the OBS path which in terms of http url.
   */
  private static String getObsUrl(String path) {
    if (path == null || path.isEmpty()) {
      return path;
    }
    int index = path.indexOf("/", 1);
    if (index < 0) {
      index = path.length();
    }
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    String bucketName = path.substring(0, index);
    return "https:/" + bucketName + "." + RestConstants.OBS_URL_SUFFIX + path
        .substring(index, path.length());
  }

  private static List<Parameter> getParameters(Config config, boolean isRequest) {
    List<Parameter> parameters = new ArrayList<>();
    List<CloudAPI> apis = config.getApis();
    Gson gson = new Gson();
    for (CloudAPI api : apis) {
      Map<String, Object> request = api.getRequest();
      if (!isRequest) {
        request = api.getResponse();
      }
      if (request != null) {
        Map<String, Object> nodeMap = request;
        Map<String, Object> dataMap = (Map<String, Object>) nodeMap.get("data");
        Map<String, Object> properties = (Map<String, Object>) dataMap.get("properties");
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
          Parameter parameter = new Parameter();
          parameter.setParam_name(entry.getKey());
          parameter.setParam_type(((Map<String, Object>) entry.getValue()).get("type").toString());
          parameter.setProtocol(api.getProtocol());
          parameters.add(parameter);
          parameter.setParam_desc(gson.toJson(entry.getValue()));
          parameter.setUrl(api.getUrl());
          parameter.setMethod(api.getMethod());
        }
      }
    }
    return parameters;
  }

  private static List<CloudAPI> getUpdatedAPIS(List<CloudAPI> apis) {
    List<CloudAPI> newApis = new ArrayList<>();
    for (CloudAPI api : apis) {
      CloudAPI newAPI = new CloudAPI();
      newAPI.setMethod(api.getMethod());
      newAPI.setProtocol(api.getProtocol());
      newAPI.setUrl(api.getUrl());
      Map<String, Object> request = api.getRequest();
      newAPI.setInput_params((Map<String, Object>) request.get("data"));
      Map<String, Object> response = api.getResponse();
      newAPI.setOutput_params((Map<String, Object>) response.get("data"));
      newApis.add(newAPI);
    }
    return newApis;
  }

  /**
   * VO object to hold information of input/output parameters.
   */
  public static class Parameter implements Serializable {

    private String url;

    private String method;

    private String protocol;

    private String param_name;

    private String param_type;

    private Double min;

    private Double max;

    private String param_desc;

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public String getMethod() {
      return method;
    }

    public void setMethod(String method) {
      this.method = method;
    }

    public String getProtocol() {
      return protocol;
    }

    public void setProtocol(String protocol) {
      this.protocol = protocol;
    }

    public String getParam_name() {
      return param_name;
    }

    public void setParam_name(String param_name) {
      this.param_name = param_name;
    }

    public String getParam_type() {
      return param_type;
    }

    public void setParam_type(String param_type) {
      this.param_type = param_type;
    }

    public Double getMin() {
      return min;
    }

    public void setMin(Double min) {
      this.min = min;
    }

    public Double getMax() {
      return max;
    }

    public void setMax(Double max) {
      this.max = max;
    }

    public String getParam_desc() {
      return param_desc;
    }

    public void setParam_desc(String param_desc) {
      this.param_desc = param_desc;
    }
  }

  /**
   *VO object holds the python library dependencies needed for this model.
   */
  public static class Dependency implements Serializable {

    private String installer;

    private List<Package> packages;

    public String getInstaller() {
      return installer;
    }

    public void setInstaller(String installer) {
      this.installer = installer;
    }

    public List<Package> getPackages() {
      return packages;
    }

    public void setPackages(List<Package> packages) {
      this.packages = packages;
    }
  }

  /**
   * VO object which holds the python package information.
   */
  public static class Package implements Serializable {

    private String package_name;

    private String package_version;

    private String restraint;

    public String getPackage_name() {
      return package_name;
    }

    public void setPackage_name(String package_name) {
      this.package_name = package_name;
    }

    public String getPackage_version() {
      return package_version;
    }

    public void setPackage_version(String package_version) {
      this.package_version = package_version;
    }

    public String getRestraint() {
      return restraint;
    }

    public void setRestraint(String restraint) {
      this.restraint = restraint;
    }
  }

  /**
   * VO object which holds the configuration information of model.
   */
  public static class Config implements Serializable {

    private String model_algorithm;

    private String model_type;

    private String runtime;

    private String swr_location;

    private Metrics metrics;

    private List<CloudAPI> apis;

    private List<Dependency> dependencies;

    public String getModel_algorithm() {
      return model_algorithm;
    }

    public void setModel_algorithm(String model_algorithm) {
      this.model_algorithm = model_algorithm;
    }

    public String getModel_type() {
      return model_type;
    }

    public void setModel_type(String model_type) {
      this.model_type = model_type;
    }

    public String getRuntime() {
      return runtime;
    }

    public void setRuntime(String runtime) {
      this.runtime = runtime;
    }

    public String getSwr_location() {
      return swr_location;
    }

    public void setSwr_location(String swr_location) {
      this.swr_location = swr_location;
    }

    public Metrics getMetrics() {
      return metrics;
    }

    public void setMetrics(Metrics metrics) {
      this.metrics = metrics;
    }

    public List<CloudAPI> getApis() {
      return apis;
    }

    public void setApis(List<CloudAPI> apis) {
      this.apis = apis;
    }

    public List<Dependency> getDependencies() {
      return dependencies;
    }

    public void setDependencies(List<Dependency> dependencies) {
      this.dependencies = dependencies;
    }
  }

  /**
   * It holds the metrics of the generated model.
   */
  public static class Metrics implements Serializable {

    private Double f1;

    private Double recall;

    private Double precision;

    private Double accuracy;

    public Double getF1() {
      return f1;
    }

    public void setF1(Double f1) {
      this.f1 = f1;
    }

    public Double getRecall() {
      return recall;
    }

    public void setRecall(Double recall) {
      this.recall = recall;
    }

    public Double getPrecision() {
      return precision;
    }

    public void setPrecision(Double precision) {
      this.precision = precision;
    }

    public Double getAccuracy() {
      return accuracy;
    }

    public void setAccuracy(Double accuracy) {
      this.accuracy = accuracy;
    }
  }

  /**
   * It holds the input/output parameter information of the model
   */
  public static class CloudAPI implements Serializable {

    private String protocol;

    private String url;

    private String method;

    private Map<String, Object> request;

    private Map<String, Object> response;

    private Map<String, Object> input_params;

    private Map<String, Object> output_params;

    public String getProtocol() {
      return protocol;
    }

    public void setProtocol(String protocol) {
      this.protocol = protocol;
    }

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public String getMethod() {
      return method;
    }

    public void setMethod(String method) {
      this.method = method;
    }

    public Map<String, Object> getRequest() {
      return request;
    }

    public void setRequest(Map<String, Object> request) {
      this.request = request;
    }

    public Map<String, Object> getResponse() {
      return response;
    }

    public void setResponse(Map<String, Object> response) {
      this.response = response;
    }

    public Map<String, Object> getInput_params() {
      return input_params;
    }

    public void setInput_params(Map<String, Object> input_params) {
      this.input_params = input_params;
    }

    public Map<String, Object> getOutput_params() {
      return output_params;
    }

    public void setOutput_params(Map<String, Object> output_params) {
      this.output_params = output_params;
    }
  }

}
