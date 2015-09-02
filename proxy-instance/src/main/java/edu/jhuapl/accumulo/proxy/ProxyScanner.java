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
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.proxy.thrift.ScanColumn;
import org.apache.accumulo.proxy.thrift.ScanOptions;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

class ProxyScanner extends AbstractProxyScanner implements Scanner {

  protected ScanOptions options;
  int batchSize = 50;

  ProxyScanner(ProxyConnector connector, ByteBuffer token, String tableName, Authorizations auths) {
    super(connector, token, tableName);
    options = new ScanOptions();
    for (ByteBuffer auth : auths.getAuthorizationsBB()) {
      options.addToAuthorizations(auth);
    }
  }

  public void addScanIterator(IteratorSetting cfg) {
    options.addToIterators(ThriftHelper.toThrift(cfg));
  }

  public void removeScanIterator(String iteratorName) {
    for (org.apache.accumulo.proxy.thrift.IteratorSetting is : options.iterators) {
      if (is.getName().equals(iteratorName)) {
        options.iterators.remove(is);
        break;
      }
    }
  }

  public void updateScanIteratorOption(String iteratorName, String key, String value) {
    for (org.apache.accumulo.proxy.thrift.IteratorSetting is : options.iterators) {
      if (is.getName().equals(iteratorName)) {
        is.putToProperties(key, value);
      }
    }
  }

  @Override
  protected void addToFetchOptions(ScanColumn col) {
    options.addToColumns(col);
  }

  public void clearColumns() {
    if (options.getColumns() != null) {
      options.getColumns().clear();
    }
  }

  public void clearScanIterators() {
    if (options.getIterators() != null) {
      options.getIterators().clear();
    }
  }

  public Iterator<Entry<Key,Value>> iterator() {
    // TODO Close if older instance open?
    try {
      scannerId = connector.getClient().createScanner(token, tableName, options);

      // TODO Someone needs to close this scannerID!
      return new ScannerIterator(scannerId, connector, batchSize);
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

  public void setTimeout(long timeOut, TimeUnit timeUnit) {
    throw ExceptionFactory.unsupported();
  }

  public long getTimeout(TimeUnit timeUnit) {
    throw ExceptionFactory.unsupported();
  }

  public void close() {
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
      LoggerFactory.getLogger(ProxyBatchScanner.class).warn("Scanner " + scannerId + " in finalize but not closed; " + "you forgot to close a scanner!");
    }
  }

  public void setRange(Range range) {
    if (range == null) {
      options.unsetRange();
    } else {
      options.setRange(ThriftHelper.toThrift(range));
    }
  }

  public Range getRange() {
    return ThriftHelper.fromThrift(options.getRange());
  }

  public void setBatchSize(int size) {
    if (size < 1) {
      throw new IllegalArgumentException("Batch size must be > 0; provided " + size);
    }
    this.batchSize = size;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void enableIsolation() {
    throw ExceptionFactory.unsupported();
  }

  public void disableIsolation() {
    throw ExceptionFactory.unsupported();
  }

  public long getReadaheadThreshold() {
    throw ExceptionFactory.unsupported();
  }

  public void setReadaheadThreshold(long batches) {
    throw ExceptionFactory.unsupported();
  }

  @Deprecated
  public void setTimeOut(int timeOut) {
    setTimeout(timeOut, TimeUnit.SECONDS);
  }

  @Deprecated
  public int getTimeOut() {
    return (int) this.getTimeout(TimeUnit.SECONDS);
  }

}
