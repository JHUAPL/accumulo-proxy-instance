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

import static edu.jhuapl.accumulo.proxy.ThriftHelper.UTF8;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.proxy.thrift.AccumuloSecurityException;
import org.apache.thrift.TException;

class ProxyConnector extends Connector {

  ProxyInstance instance;
  String principal;
  ByteBuffer token;

  ProxyConnector(ProxyInstance instance, String principal, AuthenticationToken auth) throws AccumuloSecurityException, TException {
    // TODO probably a better way to do this...
    if (!(auth instanceof PasswordToken)) {
      throw new IllegalArgumentException("Currently only works with PasswordTokens.");
    }

    this.instance = instance;
    this.principal = principal;
    String passwd = new String(((PasswordToken) auth).getPassword(), UTF8);
    Map<String,String> password = new HashMap<String,String>();
    password.put("password", passwd);
    token = instance.getClient().login(principal, password);
  }

  @Override
  public BatchScanner createBatchScanner(String tableName, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
    if (!tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName, null);
    }
    return new ProxyBatchScanner(this, token, tableName, authorizations, numQueryThreads, instance.getBatchScannerFetchSize());
  }

  @Override
  @Deprecated
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, long maxMemory, long maxLatency,
      int maxWriteThreads) throws TableNotFoundException {
    return createBatchDeleter(tableName, authorizations, numQueryThreads, new BatchWriterConfig().setMaxLatency(maxLatency, TimeUnit.MILLISECONDS)
        .setMaxMemory(maxMemory).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations, int numQueryThreads, BatchWriterConfig config)
      throws TableNotFoundException {
    throw ExceptionFactory.notYetImplemented();
  }

  @Override
  @Deprecated
  public BatchWriter createBatchWriter(String tableName, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
    return createBatchWriter(tableName,
        new BatchWriterConfig().setMaxMemory(maxMemory).setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public BatchWriter createBatchWriter(String tableName, BatchWriterConfig config) throws TableNotFoundException {
    if (!tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName, null);
    }

    try {
      return new ProxyBatchWriter(this, token, tableName, config);
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

  @Override
  @Deprecated
  public MultiTableBatchWriter createMultiTableBatchWriter(long maxMemory, long maxLatency, int maxWriteThreads) {
    return createMultiTableBatchWriter(new BatchWriterConfig().setMaxMemory(maxMemory).setMaxLatency(maxLatency, TimeUnit.MILLISECONDS)
        .setMaxWriteThreads(maxWriteThreads));
  }

  @Override
  public MultiTableBatchWriter createMultiTableBatchWriter(BatchWriterConfig config) {
    return new ProxyMultiTableBatchWriter(this, this.token, config);
  }

  @Override
  public Scanner createScanner(String tableName, Authorizations authorizations) throws TableNotFoundException {
    if (!tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName, null);
    }
    return new ProxyScanner(this, token, tableName, authorizations);
  }

  @Override
  public ConditionalWriter createConditionalWriter(String tableName, ConditionalWriterConfig config) throws TableNotFoundException {
    if (!tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName, null);
    }
    return new ProxyConditionalWriter(this, token, tableName, config);
  }

  AccumuloProxy.Iface getClient() {
    return instance.getClient();
  }

  @Override
  public Instance getInstance() {
    return instance;
  }

  @Override
  public String whoami() {
    return principal;
  }

  @Override
  public TableOperations tableOperations() {
    return new ProxyTableOperations(this, token);
  }

  @Override
  public NamespaceOperations namespaceOperations() {
    throw ExceptionFactory.unsupported();
  }

  @Override
  public SecurityOperations securityOperations() {
    return new ProxySecurityOperations(this, token);
  }

  @Override
  public InstanceOperations instanceOperations() {
    return new ProxyInstanceOperations(this, token);
  }
}
