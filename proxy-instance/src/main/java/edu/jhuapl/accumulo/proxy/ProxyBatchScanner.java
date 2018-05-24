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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.proxy.thrift.BatchScanOptions;
import org.apache.accumulo.proxy.thrift.ScanColumn;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a BatchScanner for the ProxyInstance.
 * 
 */
class ProxyBatchScanner extends AbstractProxyScanner implements BatchScanner {

  /**
   * The scan options set for this batch scanner.
   */
  protected BatchScanOptions batchOptions;

  protected int fetchSize;

  ProxyBatchScanner(ProxyConnector connector, ByteBuffer token, String tableName, Authorizations authorizations, int numQueryThreads, int fetchSize) {
    super(connector, token, tableName);
    this.fetchSize = fetchSize;

    batchOptions = new BatchScanOptions();
    batchOptions.setThreads(numQueryThreads);
    for (ByteBuffer auth : authorizations.getAuthorizationsBB()) {
      batchOptions.addToAuthorizations(auth);
    }
  }

  public void setRanges(Collection<Range> ranges) {
    if (ranges == null) {
      batchOptions.unsetRanges();
    } else {
      batchOptions.setRanges(ThriftHelper.toThriftRanges(ranges));
    }
  }

  public void addScanIterator(IteratorSetting cfg) {
    batchOptions.addToIterators(ThriftHelper.toThrift(cfg));
  }

  public void removeScanIterator(String iteratorName) {
    for (org.apache.accumulo.proxy.thrift.IteratorSetting is : batchOptions.iterators) {
      if (is.getName().equals(iteratorName)) {
        batchOptions.iterators.remove(is);
        break;
      }
    }
  }

  public void updateScanIteratorOption(String iteratorName, String key, String value) {
    for (org.apache.accumulo.proxy.thrift.IteratorSetting is : batchOptions.iterators) {
      if (is.getName().equals(iteratorName)) {
        is.putToProperties(key, value);
      }
    }
  }

  @Override
  protected void addToFetchOptions(ScanColumn col) {
    batchOptions.addToColumns(col);
  }

  public void clearColumns() {
    if (batchOptions.getColumns() != null) {
      batchOptions.getColumns().clear();
    }
  }

  public void clearScanIterators() {
    if (batchOptions.getIterators() != null) {
      batchOptions.getIterators().clear();
    }
  }

  public Iterator<Entry<Key,Value>> iterator() {
    AccumuloProxy.Iface client = connector.getClient();
    try {
      scannerId = client.createBatchScanner(token, tableName, batchOptions);
      return new ScannerIterator(scannerId, connector, fetchSize);
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

  public void close() {
    if(scannerId == null) {
      return;
    }
    try {
      connector.getClient().closeScanner(scannerId);
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    } finally {
      scannerId = null;
    }
  }

  @Override
  protected void finalize() {
    if (scannerId != null) {
      close();
      LoggerFactory.getLogger(ProxyBatchScanner.class).warn(
          "BatchScanner " + scannerId + " in finalize but not closed; " + "you forgot to close a batch scanner!");
    }
  }

}
