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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;

/**
 * Implements TableOperations as a pass through to a proxy server.
 */
class ProxyTableOperations implements TableOperations {

  ProxyConnector connector;
  AccumuloProxy.Iface client;
  ByteBuffer token;

  ProxyTableOperations(ProxyConnector connector, ByteBuffer token) {
    this.connector = connector;
    this.client = connector.getClient();
    this.token = token;
  }

  public SortedSet<String> list() {
    try {
      return new TreeSet<String>(client.listTables(token));
    } catch (TException te) {
      throw ExceptionFactory.runtimeException(te);
    }
  }

  public boolean exists(String tableName) {
    return list().contains(tableName);
  }

  public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    create(tableName, true);
  }

  public void create(String tableName, boolean limitVersion) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    create(tableName, limitVersion, TimeType.MILLIS);
  }

  public void create(String tableName, boolean versioningIter, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException {

    org.apache.accumulo.proxy.thrift.TimeType ttype = org.apache.accumulo.proxy.thrift.TimeType.valueOf(timeType.toString());
    try {
      client.createTable(token, tableName, versioningIter, ttype);
    } catch (org.apache.accumulo.proxy.thrift.TableExistsException tee) {
      throw ExceptionFactory.tableExistsException(tableName, tee);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void importTable(String tableName, String importDir) throws TableExistsException, AccumuloException, AccumuloSecurityException {
    try {
      client.importTable(token, tableName, importDir);
    } catch (org.apache.accumulo.proxy.thrift.TableExistsException tee) {
      throw ExceptionFactory.tableExistsException(tableName, tee);
    } catch (TException te) {
      throw ExceptionFactory.accumuloException(te);
    }
  }

  public void exportTable(String tableName, String exportDir) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    try {
      client.exportTable(token, tableName, exportDir);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException te) {
      throw ExceptionFactory.accumuloException(te);
    }
  }

  public void addSplits(String tableName, SortedSet<Text> partitionKeys) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    Set<ByteBuffer> tsplits = new TreeSet<ByteBuffer>();
    for (Text key : partitionKeys) {
      tsplits.add(ByteBuffer.wrap(key.getBytes()));
    }

    try {
      client.addSplits(token, tableName, tsplits);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException te) {
      throw ExceptionFactory.accumuloException(te);
    }
  }

  @Deprecated
  public Collection<Text> getSplits(String tableName) throws TableNotFoundException {
    try {
      return listSplits(tableName);
    } catch (AccumuloSecurityException e) {
      throw ExceptionFactory.runtimeException(e);
    } catch (AccumuloException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

  public Collection<Text> listSplits(String tableName) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    return listSplits(tableName, Integer.MAX_VALUE);
  }

  @Deprecated
  public Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException {
    try {
      return listSplits(tableName, maxSplits);
    } catch (AccumuloSecurityException e) {
      throw ExceptionFactory.runtimeException(e);
    } catch (AccumuloException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

  public Collection<Text> listSplits(String tableName, int maxSplits) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    try {
      List<ByteBuffer> list = client.listSplits(token, tableName, maxSplits);

      List<Text> text = new ArrayList<Text>(list.size());
      for (ByteBuffer bb : list) {
        text.add(new Text(bb.array()));
      }
      return text;
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException te) {
      throw ExceptionFactory.accumuloException(te);
    }
  }

  public Text getMaxRow(String tableName, Authorizations auths, Text startRow, boolean startInclusive, Text endRow, boolean endInclusive)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    try {
      ByteBuffer answer = client.getMaxRow(token, tableName, new HashSet<ByteBuffer>(auths.getAuthorizationsBB()), ThriftHelper.toByteBuffer(startRow),
          startInclusive, ThriftHelper.toByteBuffer(endRow), endInclusive);
      return new Text(answer.array());
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void merge(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    try {
      client.mergeTablets(token, tableName, ThriftHelper.toByteBuffer(start), ThriftHelper.toByteBuffer(end));
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void deleteRows(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    try {
      client.deleteRows(token, tableName, ThriftHelper.toByteBuffer(start), ThriftHelper.toByteBuffer(end));
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void compact(String tableName, Text start, Text end, boolean flush, boolean wait) throws AccumuloSecurityException, TableNotFoundException,
      AccumuloException {
    compact(tableName, start, end, null, flush, wait);
  }

  public void compact(String tableName, Text start, Text end, List<IteratorSetting> iterators, boolean flush, boolean wait) throws AccumuloSecurityException,
      TableNotFoundException, AccumuloException {
    try {
      client.compactTable(token, tableName, ThriftHelper.toByteBuffer(start), ThriftHelper.toByteBuffer(end), ThriftHelper.toThriftIteratorSettings(iterators),
          flush, wait);
    } catch (org.apache.accumulo.proxy.thrift.AccumuloSecurityException e) {
      throw new AccumuloSecurityException(connector.whoami(), SecurityErrorCode.DEFAULT_SECURITY_ERROR, "", e);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (org.apache.accumulo.proxy.thrift.AccumuloException e) {
      throw ExceptionFactory.accumuloException(e);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void cancelCompaction(String tableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    try {
      client.cancelCompaction(token, tableName);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void delete(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    try {
      client.deleteTable(token, tableName);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void clone(String srcTableName, String newTableName, boolean flush, Map<String,String> propertiesToSet, Set<String> propertiesToExclude)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
    try {
      client.cloneTable(token, srcTableName, newTableName, flush, propertiesToSet, propertiesToExclude);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(srcTableName, tnfe);
    } catch (org.apache.accumulo.proxy.thrift.TableExistsException tee) {
      throw ExceptionFactory.tableExistsException(newTableName, tee);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void rename(String oldTableName, String newTableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
      TableExistsException {
    try {
      client.renameTable(token, oldTableName, newTableName);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(oldTableName, tnfe);
    } catch (org.apache.accumulo.proxy.thrift.TableExistsException tee) {
      throw ExceptionFactory.tableExistsException(newTableName, tee);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  @Deprecated
  public void flush(String tableName) throws AccumuloException, AccumuloSecurityException {
    try {
      flush(tableName, null, null, true);
    } catch (TableNotFoundException tnfe) {
      throw ExceptionFactory.accumuloException(tnfe);
    }
  }

  public void flush(String tableName, Text start, Text end, boolean wait) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    try {
      client.flushTable(token, tableName, ThriftHelper.toByteBuffer(start), ThriftHelper.toByteBuffer(end), wait);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void setProperty(String tableName, String property, String value) throws AccumuloException, AccumuloSecurityException {
    try {
      client.setProperty(token, property, value);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void removeProperty(String tableName, String property) throws AccumuloException, AccumuloSecurityException {
    try {
      client.removeProperty(token, property);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public Iterable<Entry<String,String>> getProperties(String tableName) throws AccumuloException, TableNotFoundException {
    try {
      return client.getTableProperties(token, tableName).entrySet();
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void setLocalityGroups(String tableName, Map<String,Set<Text>> groups) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    try {
      Map<String,Set<String>> tgroups = new HashMap<String,Set<String>>();
      for (Entry<String,Set<Text>> entry : groups.entrySet()) {
        Set<String> tset = new HashSet<String>();
        tgroups.put(entry.getKey(), tset);
        for (Text t : entry.getValue()) {
          tset.add(t.toString());
        }
      }
      client.setLocalityGroups(token, tableName, tgroups);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public Map<String,Set<Text>> getLocalityGroups(String tableName) throws AccumuloException, TableNotFoundException {
    try {
      Map<String,Set<String>> tgroups = client.getLocalityGroups(token, tableName);
      Map<String,Set<Text>> groups = new HashMap<String,Set<Text>>();
      for (Entry<String,Set<String>> tentry : tgroups.entrySet()) {
        Set<Text> set = new HashSet<Text>();
        groups.put(tentry.getKey(), set);
        for (String t : tentry.getValue()) {
          set.add(new Text(t));
        }
      }
      return groups;
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    try {
      return ThriftHelper.fromThriftRanges(client.splitRangeByTablets(token, tableName, ThriftHelper.toThrift(range), maxSplits));
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void importDirectory(String tableName, String dir, String failureDir, boolean setTime) throws TableNotFoundException, IOException, AccumuloException,
      AccumuloSecurityException {
    try {
      client.importDirectory(token, tableName, dir, failureDir, setTime);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void offline(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    offline(tableName, true);
  }

  public void offline(String tableName, boolean wait) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    try {
      client.offlineTable(token, tableName, wait);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void online(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    online(tableName, true);
  }

  public void online(String tableName, boolean wait) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    try {
      client.onlineTable(token, tableName, wait);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }

  }

  public void clearLocatorCache(String tableName) throws TableNotFoundException {
    try {
      client.clearLocatorCache(token, tableName);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

  public Map<String,String> tableIdMap() {
    try {
      return client.tableIdMap(token);
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

  public void attachIterator(String tableName, IteratorSetting setting) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    attachIterator(tableName, setting, null);
  }

  public void attachIterator(String tableName, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException,
      TableNotFoundException {
    try {
      client.attachIterator(token, tableName, ThriftHelper.toThrift(setting), ThriftHelper.toThrift(scopes));
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void removeIterator(String tableName, String name, EnumSet<IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException,
      TableNotFoundException {
    try {
      client.removeIterator(token, tableName, name, ThriftHelper.toThrift(scopes));
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public IteratorSetting getIteratorSetting(String tableName, String name, IteratorScope scope) throws AccumuloSecurityException, AccumuloException,
      TableNotFoundException {
    try {
      return ThriftHelper.fromThrift(client.getIteratorSetting(token, tableName, name, ThriftHelper.toThrift(scope)));
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public Map<String,EnumSet<IteratorScope>> listIterators(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    try {
      return ThriftHelper.fromThrift(client.listIterators(token, tableName));
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void checkIteratorConflicts(String tableName, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloException, TableNotFoundException {
    try {
      client.checkIteratorConflicts(token, tableName, ThriftHelper.toThrift(setting), ThriftHelper.toThrift(scopes));
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public int addConstraint(String tableName, String constraintClassName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    try {
      return client.addConstraint(token, tableName, constraintClassName);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void removeConstraint(String tableName, int constraint) throws AccumuloException, AccumuloSecurityException {
    try {
      client.removeConstraint(token, tableName, constraint);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public Map<String,Integer> listConstraints(String tableName) throws AccumuloException, TableNotFoundException {
    try {
      return client.listConstraints(token, tableName);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public List<DiskUsage> getDiskUsage(Set<String> tables) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    try {
      return new ArrayList<DiskUsage>(ThriftHelper.fromThriftDiskUsage(client.getDiskUsage(token, tables)));
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public boolean testClassLoad(String tableName, String className, String asTypeName) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    try {
      return client.testClassLoad(token, className, asTypeName);
    } catch (org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
      throw ExceptionFactory.tableNotFoundException(tableName, tnfe);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

}
