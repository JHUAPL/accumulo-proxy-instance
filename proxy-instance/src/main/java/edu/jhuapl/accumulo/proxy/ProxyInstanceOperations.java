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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.thrift.TException;

/**
 * Provides a pass through for InstaceOperations to a proxy server.
 */
class ProxyInstanceOperations implements InstanceOperations {

  ProxyConnector connector;
  ByteBuffer token;

  ProxyInstanceOperations(ProxyConnector connector, ByteBuffer token) {
    this.connector = connector;
    this.token = token;
  }

  public void setProperty(String property, String value) throws AccumuloException, AccumuloSecurityException {
    try {
      connector.getClient().setProperty(token, property, value);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void removeProperty(String property) throws AccumuloException, AccumuloSecurityException {
    try {
      connector.getClient().removeProperty(token, property);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public Map<String,String> getSystemConfiguration() throws AccumuloException, AccumuloSecurityException {
    try {
      return connector.getClient().getSystemConfiguration(token);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public Map<String,String> getSiteConfiguration() throws AccumuloException, AccumuloSecurityException {
    try {
      return connector.getClient().getSiteConfiguration(token);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public List<String> getTabletServers() {
    try {
      return connector.getClient().getTabletServers(token);
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

  public List<ActiveScan> getActiveScans(String tserver) throws AccumuloException, AccumuloSecurityException {
    try {
      return ThriftHelper.fromThriftActiveScans(connector.getClient().getActiveScans(token, tserver));
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public List<ActiveCompaction> getActiveCompactions(String tserver) throws AccumuloException, AccumuloSecurityException {
    try {
      return ThriftHelper.fromThriftActiveCompactions(connector.tableOperations().tableIdMap(), connector.getClient().getActiveCompactions(token, tserver));
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void ping(String tserver) throws AccumuloException {
    try {
      connector.getClient().pingTabletServer(token, tserver);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public boolean testClassLoad(String className, String asTypeName) throws AccumuloException, AccumuloSecurityException {
    try {
      return connector.getClient().testClassLoad(token, className, asTypeName);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

}
