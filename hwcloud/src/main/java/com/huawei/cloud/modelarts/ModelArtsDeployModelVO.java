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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import org.apache.spark.sql.aisql.intf.ModelAPIException;

/**
 * It the VO object which can holds all necessary information to deploy the model.
 */
public class ModelArtsDeployModelVO implements Serializable {

  private static final long serialVersionUID = -2543230946978291602L;

  private String service_name;

  private String infer_type = "real-time";

  private String workspace_id = "0";

  private String vpc_id;

  private String subnet_network_id;

  private String security_group_id;

  private String cluster_id;

  private List<RealTimeConfig> config = new ArrayList<>();

  public String getService_name() {
    return service_name;
  }

  public void setService_name(String service_name) {
    this.service_name = service_name;
  }

  public String getInfer_type() {
    return infer_type;
  }

  public void setInfer_type(String infer_type) {
    this.infer_type = infer_type;
  }

  public String getWorkspace_id() {
    return workspace_id;
  }

  public void setWorkspace_id(String workspace_id) {
    this.workspace_id = workspace_id;
  }

  public String getVpc_id() {
    return vpc_id;
  }

  public void setVpc_id(String vpc_id) {
    this.vpc_id = vpc_id;
  }

  public String getSubnet_network_id() {
    return subnet_network_id;
  }

  public void setSubnet_network_id(String subnet_network_id) {
    this.subnet_network_id = subnet_network_id;
  }

  public String getSecurity_group_id() {
    return security_group_id;
  }

  public void setSecurity_group_id(String security_group_id) {
    this.security_group_id = security_group_id;
  }

  public String getCluster_id() {
    return cluster_id;
  }

  public void setCluster_id(String cluster_id) {
    this.cluster_id = cluster_id;
  }

  public List<RealTimeConfig> getConfig() {
    return config;
  }

  public void setConfig(List<RealTimeConfig> config) {
    this.config = config;
  }

  public static String generateJson(Map<String, String> options, String modelName)
      throws ModelAPIException {
    String model_id = options.get("model_id");
    if (model_id == null) {
      throw new ModelAPIException("First import the model to get that model_id");
    }
    ModelArtsDeployModelVO modelVO = new ModelArtsDeployModelVO();
    modelVO.setService_name(modelName);
    RealTimeConfig config = new RealTimeConfig();
    config.setModel_id(model_id);
    modelVO.getConfig().add(config);
    Gson gson = new Gson();
    return gson.toJson(modelVO);
  }

  public static class RealTimeConfig implements Serializable {

    private static final long serialVersionUID = 2181548928130839485L;

    private String model_id;

    private Integer weight = 100;

    private String specification = "modelarts.vm.cpu.2u";

    private Integer instance_count = 1;

    private Map<String, String> envs = new HashMap<>();

    public String getModel_id() {
      return model_id;
    }

    public void setModel_id(String model_id) {
      this.model_id = model_id;
    }

    public Integer getWeight() {
      return weight;
    }

    public void setWeight(Integer weight) {
      this.weight = weight;
    }

    public String getSpecification() {
      return specification;
    }

    public void setSpecification(String specification) {
      this.specification = specification;
    }

    public Integer getInstance_count() {
      return instance_count;
    }

    public void setInstance_count(Integer instance_count) {
      this.instance_count = instance_count;
    }

    public Map<String, String> getEnvs() {
      return envs;
    }

    public void setEnvs(Map<String, String> envs) {
      this.envs = envs;
    }
  }

}
