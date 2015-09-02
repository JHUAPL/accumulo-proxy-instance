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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.proxy.thrift.ActiveCompaction;
import org.apache.accumulo.proxy.thrift.ActiveScan;
import org.apache.accumulo.proxy.thrift.BatchScanOptions;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.ConditionalStatus;
import org.apache.accumulo.proxy.thrift.ConditionalUpdates;
import org.apache.accumulo.proxy.thrift.ConditionalWriterOptions;
import org.apache.accumulo.proxy.thrift.DiskUsage;
import org.apache.accumulo.proxy.thrift.IteratorScope;
import org.apache.accumulo.proxy.thrift.IteratorSetting;
import org.apache.accumulo.proxy.thrift.Key;
import org.apache.accumulo.proxy.thrift.KeyValueAndPeek;
import org.apache.accumulo.proxy.thrift.MutationsRejectedException;
import org.apache.accumulo.proxy.thrift.NoMoreEntriesException;
import org.apache.accumulo.proxy.thrift.PartialKey;
import org.apache.accumulo.proxy.thrift.Range;
import org.apache.accumulo.proxy.thrift.ScanOptions;
import org.apache.accumulo.proxy.thrift.ScanResult;
import org.apache.accumulo.proxy.thrift.SystemPermission;
import org.apache.accumulo.proxy.thrift.TableExistsException;
import org.apache.accumulo.proxy.thrift.TableNotFoundException;
import org.apache.accumulo.proxy.thrift.TablePermission;
import org.apache.accumulo.proxy.thrift.TimeType;
import org.apache.accumulo.proxy.thrift.UnknownScanner;
import org.apache.accumulo.proxy.thrift.UnknownWriter;
import org.apache.accumulo.proxy.thrift.WriterOptions;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.LoggerFactory;

/**
 * A proxy instance that uses the Accumulo Proxy Thrift interface to fulfill the Accumulo client APIs. Note this instance implements Closeable and, while not
 * part of the public Instance API, should be closed when no longer needed.
 */
public class ProxyInstance implements Instance, Closeable {

  /**
   * Default fetch size to be used for BatchScanners. Currently equal to 1,000.
   */
  private static final int DEFAULT_FETCH_SIZE = 1_000;

  private AccumuloProxy.Iface client;

  private TTransport transport;
  private int fetchSize;

  /**
   * Assumes a TSocket transport wrapped by a TFramedTransport.
   * 
   * @param host
   *          the host name or IP address where the Accumulo Thrift Proxy server is running
   * @param port
   *          the port where the Accumulo Thrift Proxy server is listening
   * @throws TTransportException
   *           thrown if the Thrift TTransport cannot be established.
   */
  public ProxyInstance(String host, int port) throws TTransportException {
    this(host, port, DEFAULT_FETCH_SIZE);
  }

  /**
   * Assumes a TSocket transport wrapped by a TFramedTransport.
   * 
   * @param host
   *          the host name or IP address where the Accumulo Thrift Proxy server is running
   * @param port
   *          the port where the Accumulo Thrift Proxy server is listening
   * @param fetchSize
   *          the fetch size for BatchScanners
   * @throws TTransportException
   *           thrown if the Thrift TTransport cannot be established.
   */
  public ProxyInstance(String host, int port, int fetchSize) throws TTransportException {
    this(new TFramedTransport(new TSocket(host, port)), fetchSize);
  }

  public ProxyInstance(TTransport transport) throws TTransportException {
    this(transport, DEFAULT_FETCH_SIZE);
  }

  /**
   * 
   * @param transport
   *          Thrift transport to communicate with Proxy Server
   * @param fetchSize
   *          the fetch size for BatchScanners. Must be 0 < fetchSize <= 2000. If fetchSize is outside of this range, a warning will be logged and the
   *          {@link #DEFAULT_FETCH_SIZE} will be used.
   * @throws TTransportException
   *           thrown if the Thrift TTransport cannot be established.
   */
  public ProxyInstance(TTransport transport, int fetchSize) throws TTransportException {
    if (!transport.isOpen()) {
      transport.open();
    }
    TProtocol protocol = new TCompactProtocol(transport);
    client = new SynchronizedProxy(new AccumuloProxy.Client(protocol));
    this.transport = transport;

    if (fetchSize <= 0 || fetchSize > 2000) {
      LoggerFactory.getLogger(ProxyInstance.class).warn(
          "Fetch size out of range (0 < fetchSize <= 2000): " + fetchSize + "; using default: " + DEFAULT_FETCH_SIZE);
      this.fetchSize = DEFAULT_FETCH_SIZE;
    } else {
      this.fetchSize = fetchSize;
    }
  }

  public int getBatchScannerFetchSize() {
    return fetchSize;
  }

  public String getRootTabletLocation() {
    throw ExceptionFactory.unsupported();
  }

  public List<String> getMasterLocations() {
    throw ExceptionFactory.unsupported();
  }

  public String getInstanceID() {
    throw ExceptionFactory.unsupported();
  }

  public String getInstanceName() {
    throw ExceptionFactory.unsupported();
  }

  public String getZooKeepers() {
    throw ExceptionFactory.unsupported();
  }

  public int getZooKeepersSessionTimeOut() {
    throw ExceptionFactory.unsupported();
  }

  @Deprecated
  public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, new PasswordToken(pass));
  }

  @Deprecated
  public Connector getConnector(String user, ByteBuffer pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, new PasswordToken(pass));
  }

  @Deprecated
  public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
    return getConnector(user, new PasswordToken(pass));
  }

  @Deprecated
  public AccumuloConfiguration getConfiguration() {
    throw ExceptionFactory.unsupported();
  }

  @Deprecated
  public void setConfiguration(AccumuloConfiguration conf) {
    throw ExceptionFactory.unsupported();
  }

  public Connector getConnector(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    try {
      return new ProxyConnector(this, principal, token);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  AccumuloProxy.Iface getClient() {
    return client;
  }

  public void close() {
    // TODO- Neither Instance API nor Connector have a "close" method. But
    // we need to close the transport when done. How to handle it? Currently
    // clients must cast their Instance as a ProxyInstance and call close,
    // *hope* the finalize method is called, or just be a bad citizen and
    // not clean up resources on the proxy server.
    if (transport != null) {
      try {
        transport.close();
      } finally {
        transport = null;
        client = null;
      }
    }
  }

  @Override
  protected void finalize() {
    close();
  }

  /**
   * A wrapper class that synchronizes every method in the Iface interface against this object, and then passes the call through to the wrapped Iface delegate.
   * Due to the asynchronous nature of Thrift internals, if multiple threads use the same Iface object, the server may receive requests out-of-order and fail.
   * This class helps mitigate that risk by ensuring only one thread is ever communicating with the proxy at one time. This incurs a performance hit, but if you
   * are using the proxy instance, you probably aren't too concerned about throughput anyway...
   */
  private static class SynchronizedProxy implements AccumuloProxy.Iface {

    AccumuloProxy.Iface delegate;

    SynchronizedProxy(AccumuloProxy.Iface delegate) {
      this.delegate = delegate;
    }

    @Override
    public synchronized ByteBuffer login(String principal, Map<String,String> loginProperties)
        throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.login(principal, loginProperties);
    }

    @Override
    public synchronized int addConstraint(ByteBuffer login, String tableName, String constraintClassName)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.addConstraint(login, tableName, constraintClassName);
    }

    @Override
    public synchronized void addSplits(ByteBuffer login, String tableName, Set<ByteBuffer> splits) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      delegate.addSplits(login, tableName, splits);
    }

    @Override
    public synchronized void attachIterator(ByteBuffer login, String tableName, IteratorSetting setting, Set<IteratorScope> scopes)
        throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.AccumuloException, TableNotFoundException,
        TException {
      delegate.attachIterator(login, tableName, setting, scopes);
    }

    @Override
    public synchronized void checkIteratorConflicts(ByteBuffer login, String tableName, IteratorSetting setting, Set<IteratorScope> scopes)
        throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.AccumuloException, TableNotFoundException,
        TException {
      delegate.checkIteratorConflicts(login, tableName, setting, scopes);
    }

    @Override
    public synchronized void clearLocatorCache(ByteBuffer login, String tableName) throws TableNotFoundException, TException {
      delegate.clearLocatorCache(login, tableName);
    }

    @Override
    public synchronized void cloneTable(ByteBuffer login, String tableName, String newTableName, boolean flush, Map<String,String> propertiesToSet,
        Set<String> propertiesToExclude) throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
        TableNotFoundException, TableExistsException, TException {
      delegate.cloneTable(login, tableName, newTableName, flush, propertiesToSet, propertiesToExclude);
    }

    @Override
    public synchronized void compactTable(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow, List<IteratorSetting> iterators,
        boolean flush, boolean wait) throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        org.apache.accumulo.proxy.thrift.AccumuloException, TException {
      delegate.compactTable(login, tableName, startRow, endRow, iterators, flush, wait);
    }

    @Override
    public synchronized void cancelCompaction(ByteBuffer login, String tableName) throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
        TableNotFoundException, org.apache.accumulo.proxy.thrift.AccumuloException, TException {
      delegate.cancelCompaction(login, tableName);
    }

    @Override
    public synchronized void createTable(ByteBuffer login, String tableName, boolean versioningIter, TimeType type)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableExistsException, TException {
      delegate.createTable(login, tableName, versioningIter, type);
    }

    @Override
    public synchronized void deleteTable(ByteBuffer login, String tableName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      delegate.deleteTable(login, tableName);
    }

    @Override
    public synchronized void deleteRows(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      delegate.deleteRows(login, tableName, startRow, endRow);
    }

    @Override
    public synchronized void exportTable(ByteBuffer login, String tableName, String exportDir) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      delegate.exportTable(login, tableName, exportDir);
    }

    @Override
    public synchronized void flushTable(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow, boolean wait)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      delegate.flushTable(login, tableName, startRow, endRow, wait);
    }

    @Override
    public synchronized List<DiskUsage> getDiskUsage(ByteBuffer login, Set<String> tables) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      return delegate.getDiskUsage(login, tables);
    }

    @Override
    public synchronized Map<String,Set<String>> getLocalityGroups(ByteBuffer login, String tableName)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.getLocalityGroups(login, tableName);
    }

    @Override
    public synchronized IteratorSetting getIteratorSetting(ByteBuffer login, String tableName, String iteratorName, IteratorScope scope)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.getIteratorSetting(login, tableName, iteratorName, scope);
    }

    @Override
    public synchronized ByteBuffer getMaxRow(ByteBuffer login, String tableName, Set<ByteBuffer> auths, ByteBuffer startRow, boolean startInclusive,
        ByteBuffer endRow, boolean endInclusive) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      return delegate.getMaxRow(login, tableName, auths, startRow, startInclusive, endRow, endInclusive);
    }

    @Override
    public synchronized Map<String,String> getTableProperties(ByteBuffer login, String tableName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      return delegate.getTableProperties(login, tableName);
    }

    @Override
    public synchronized void importDirectory(ByteBuffer login, String tableName, String importDir, String failureDir, boolean setTime)
        throws TableNotFoundException, org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
        TException {
      delegate.importDirectory(login, tableName, importDir, failureDir, setTime);
    }

    @Override
    public synchronized void importTable(ByteBuffer login, String tableName, String importDir) throws TableExistsException,
        org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      delegate.importTable(login, tableName, importDir);
    }

    @Override
    public synchronized List<ByteBuffer> listSplits(ByteBuffer login, String tableName, int maxSplits)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.listSplits(login, tableName, maxSplits);
    }

    @Override
    public synchronized Set<String> listTables(ByteBuffer login) throws TException {
      return delegate.listTables(login);
    }

    @Override
    public synchronized Map<String,Set<IteratorScope>> listIterators(ByteBuffer login, String tableName)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.listIterators(login, tableName);
    }

    @Override
    public synchronized Map<String,Integer> listConstraints(ByteBuffer login, String tableName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      return delegate.listConstraints(login, tableName);
    }

    @Override
    public synchronized void mergeTablets(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      delegate.mergeTablets(login, tableName, startRow, endRow);
    }

    @Override
    public synchronized void offlineTable(ByteBuffer login, String tableName, boolean wait) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      delegate.offlineTable(login, tableName, wait);
    }

    @Override
    public synchronized void onlineTable(ByteBuffer login, String tableName, boolean wait) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      delegate.onlineTable(login, tableName, wait);
    }

    @Override
    public synchronized void removeConstraint(ByteBuffer login, String tableName, int constraint) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      delegate.removeConstraint(login, tableName, constraint);
    }

    @Override
    public synchronized void removeIterator(ByteBuffer login, String tableName, String iterName, Set<IteratorScope> scopes)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      delegate.removeIterator(login, tableName, iterName, scopes);
    }

    @Override
    public synchronized void removeTableProperty(ByteBuffer login, String tableName, String property)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      delegate.removeTableProperty(login, tableName, property);
    }

    @Override
    public synchronized void renameTable(ByteBuffer login, String oldTableName, String newTableName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TableExistsException, TException {
      delegate.renameTable(login, oldTableName, newTableName);
    }

    @Override
    public synchronized void setLocalityGroups(ByteBuffer login, String tableName, Map<String,Set<String>> groups)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      delegate.setLocalityGroups(login, tableName, groups);
    }

    @Override
    public synchronized void setTableProperty(ByteBuffer login, String tableName, String property, String value)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      delegate.setTableProperty(login, tableName, property, value);
    }

    @Override
    public synchronized Set<Range> splitRangeByTablets(ByteBuffer login, String tableName, Range range, int maxSplits)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.splitRangeByTablets(login, tableName, range, maxSplits);
    }

    @Override
    public synchronized boolean tableExists(ByteBuffer login, String tableName) throws TException {
      return delegate.tableExists(login, tableName);
    }

    @Override
    public synchronized Map<String,String> tableIdMap(ByteBuffer login) throws TException {
      return delegate.tableIdMap(login);
    }

    @Override
    public synchronized boolean testTableClassLoad(ByteBuffer login, String tableName, String className, String asTypeName)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.testClassLoad(login, className, asTypeName);
    }

    @Override
    public synchronized void pingTabletServer(ByteBuffer login, String tserver) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      delegate.pingTabletServer(login, tserver);
    }

    @Override
    public synchronized List<ActiveScan> getActiveScans(ByteBuffer login, String tserver) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.getActiveScans(login, tserver);
    }

    @Override
    public synchronized List<ActiveCompaction> getActiveCompactions(ByteBuffer login, String tserver)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.getActiveCompactions(login, tserver);
    }

    @Override
    public synchronized Map<String,String> getSiteConfiguration(ByteBuffer login) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.getSiteConfiguration(login);
    }

    @Override
    public synchronized Map<String,String> getSystemConfiguration(ByteBuffer login) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.getSystemConfiguration(login);
    }

    @Override
    public synchronized List<String> getTabletServers(ByteBuffer login) throws TException {
      return delegate.getTabletServers(login);
    }

    @Override
    public synchronized void removeProperty(ByteBuffer login, String property) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      delegate.removeProperty(login, property);
    }

    @Override
    public synchronized void setProperty(ByteBuffer login, String property, String value) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      delegate.setProperty(login, property, value);
    }

    @Override
    public synchronized boolean testClassLoad(ByteBuffer login, String className, String asTypeName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.testClassLoad(login, className, asTypeName);
    }

    @Override
    public synchronized boolean authenticateUser(ByteBuffer login, String user, Map<String,String> properties)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.authenticateUser(login, user, properties);
    }

    @Override
    public synchronized void changeUserAuthorizations(ByteBuffer login, String user, Set<ByteBuffer> authorizations)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      delegate.changeUserAuthorizations(login, user, authorizations);
    }

    @Override
    public synchronized void changeLocalUserPassword(ByteBuffer login, String user, ByteBuffer password)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      delegate.changeLocalUserPassword(login, user, password);
    }

    @Override
    public synchronized void createLocalUser(ByteBuffer login, String user, ByteBuffer password) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      delegate.createLocalUser(login, user, password);
    }

    @Override
    public synchronized void dropLocalUser(ByteBuffer login, String user) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      delegate.dropLocalUser(login, user);
    }

    @Override
    public synchronized List<ByteBuffer> getUserAuthorizations(ByteBuffer login, String user) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.getUserAuthorizations(login, user);
    }

    @Override
    public synchronized void grantSystemPermission(ByteBuffer login, String user, SystemPermission perm)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      delegate.grantSystemPermission(login, user, perm);
    }

    @Override
    public synchronized void grantTablePermission(ByteBuffer login, String user, String table, TablePermission perm)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      delegate.grantTablePermission(login, user, table, perm);
    }

    @Override
    public synchronized boolean hasSystemPermission(ByteBuffer login, String user, SystemPermission perm)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.hasSystemPermission(login, user, perm);
    }

    @Override
    public synchronized boolean hasTablePermission(ByteBuffer login, String user, String table, TablePermission perm)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.hasTablePermission(login, user, table, perm);
    }

    @Override
    public synchronized Set<String> listLocalUsers(ByteBuffer login) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      return delegate.listLocalUsers(login);
    }

    @Override
    public synchronized void revokeSystemPermission(ByteBuffer login, String user, SystemPermission perm)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      delegate.revokeSystemPermission(login, user, perm);
    }

    @Override
    public synchronized void revokeTablePermission(ByteBuffer login, String user, String table, TablePermission perm)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      delegate.revokeTablePermission(login, user, table, perm);
    }

    @Override
    public synchronized String createBatchScanner(ByteBuffer login, String tableName, BatchScanOptions options)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.createBatchScanner(login, tableName, options);
    }

    @Override
    public synchronized String createScanner(ByteBuffer login, String tableName, ScanOptions options)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.createScanner(login, tableName, options);
    }

    @Override
    public synchronized boolean hasNext(String scanner) throws UnknownScanner, TException {
      return delegate.hasNext(scanner);
    }

    @Override
    public synchronized KeyValueAndPeek nextEntry(String scanner) throws NoMoreEntriesException, UnknownScanner,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.nextEntry(scanner);
    }

    @Override
    public synchronized ScanResult nextK(String scanner, int k) throws NoMoreEntriesException, UnknownScanner,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.nextK(scanner, k);
    }

    @Override
    public synchronized void closeScanner(String scanner) throws UnknownScanner, TException {
      delegate.closeScanner(scanner);
    }

    @Override
    public synchronized void updateAndFlush(ByteBuffer login, String tableName, Map<ByteBuffer,List<ColumnUpdate>> cells)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        MutationsRejectedException, TException {
      delegate.updateAndFlush(login, tableName, cells);
    }

    @Override
    public synchronized String createWriter(ByteBuffer login, String tableName, WriterOptions opts) throws org.apache.accumulo.proxy.thrift.AccumuloException,
        org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException, TException {
      return delegate.createWriter(login, tableName, opts);
    }

    @Override
    public synchronized void update(String writer, Map<ByteBuffer,List<ColumnUpdate>> cells) throws TException {
      delegate.update(writer, cells);
    }

    @Override
    public synchronized void flush(String writer) throws UnknownWriter, MutationsRejectedException, TException {
      delegate.flush(writer);
    }

    @Override
    public synchronized void closeWriter(String writer) throws UnknownWriter, MutationsRejectedException, TException {
      delegate.closeWriter(writer);
    }

    @Override
    public synchronized ConditionalStatus updateRowConditionally(ByteBuffer login, String tableName, ByteBuffer row, ConditionalUpdates updates)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.updateRowConditionally(login, tableName, row, updates);
    }

    @Override
    public synchronized String createConditionalWriter(ByteBuffer login, String tableName, ConditionalWriterOptions options)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TableNotFoundException,
        TException {
      return delegate.createConditionalWriter(login, tableName, options);
    }

    @Override
    public synchronized Map<ByteBuffer,ConditionalStatus> updateRowsConditionally(String conditionalWriter, Map<ByteBuffer,ConditionalUpdates> updates)
        throws UnknownWriter, org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
      return delegate.updateRowsConditionally(conditionalWriter, updates);
    }

    @Override
    public synchronized void closeConditionalWriter(String conditionalWriter) throws TException {
      delegate.closeConditionalWriter(conditionalWriter);
    }

    @Override
    public synchronized Range getRowRange(ByteBuffer row) throws TException {
      return delegate.getRowRange(row);
    }

    @Override
    public synchronized Key getFollowing(Key key, PartialKey part) throws TException {
      return delegate.getFollowing(key, part);
    }
  }

}
