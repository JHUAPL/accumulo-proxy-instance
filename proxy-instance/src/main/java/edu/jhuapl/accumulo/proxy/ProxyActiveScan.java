/**
 * Copyright 2014-2015 The Johns Hopkins University / Applied Physics Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.jhuapl.accumulo.proxy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.ScanState;
import org.apache.accumulo.core.client.admin.ScanType;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Implementation of an ActiveScan for the ProxyInstance. This is basically a data structure to hold all of the provided details about an active scan.
 */
class ProxyActiveScan extends ActiveScan {

  long scanId;
  String client;
  String user;
  String table;
  long age;
  long lastContactTime;
  ScanType type;
  ScanState state;
  KeyExtent extent;
  List<Column> columns;
  Authorizations authorizations;
  long idleTime;

  ProxyActiveScan(long scanId, String client, String user, String table, long age, long lastContactTime, ScanType type, ScanState state, KeyExtent extent,
      List<Column> columns, List<ByteBuffer> authorizations, long idleTime) {
    super();
    this.scanId = scanId;
    this.client = client;
    this.user = user;
    this.table = table;
    this.age = age;
    this.lastContactTime = lastContactTime;
    this.type = type;
    this.state = state;
    this.extent = extent;
    this.columns = columns;
    this.authorizations = new Authorizations(authorizations);
    this.idleTime = idleTime;
  }

  @Override
  public long getScanid() {
    return scanId;
  }

  @Override
  public String getClient() {
    return client;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public String getTable() {
    return table;
  }

  @Override
  public long getAge() {
    return age;
  }

  @Override
  public long getLastContactTime() {
    return lastContactTime;
  }

  @Override
  public ScanType getType() {
    return type;
  }

  @Override
  public ScanState getState() {
    return state;
  }

  @Override
  public KeyExtent getExtent() {
    return extent;
  }

  @Override
  public List<Column> getColumns() {
    return columns;
  }

  @Override
  public List<String> getSsiList() {
    throw ExceptionFactory.unsupported();
  }

  @Override
  public Map<String,Map<String,String>> getSsio() {
    throw ExceptionFactory.unsupported();
  }

  @Override
  public Authorizations getAuthorizations() {
    return authorizations;
  }

  @Override
  public long getIdleTime() {
    return idleTime;
  }

  @Override
  public String toString() {
    return "ProxyActiveScan [scanId=" + scanId + ", client=" + client + ", user=" + user + ", table=" + table + ", age=" + age + ", lastContactTime="
        + lastContactTime + ", type=" + type + ", state=" + state + ", extent=" + extent + ", columns=" + columns + ", authorizations=" + authorizations
        + ", idleTime=" + idleTime + "]";
  }

}
