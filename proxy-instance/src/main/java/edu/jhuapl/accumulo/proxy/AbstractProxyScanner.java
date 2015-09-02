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
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.proxy.thrift.ScanColumn;
import org.apache.hadoop.io.Text;

/**
 * Parent class for proxy scanners.
 */
abstract class AbstractProxyScanner implements ScannerBase {

  /**
   * The connector that created this scanner.
   */
  protected ProxyConnector connector;

  /**
   * The token used when making proxy requests.
   */
  protected ByteBuffer token;

  /**
   * Table name for this scanner.
   */
  protected String tableName;

  /**
   * Id assigned to this scanner by the proxy server.
   */
  protected String scannerId = null;

  protected AbstractProxyScanner(ProxyConnector connector, ByteBuffer token, String tableName) {
    this.connector = connector;

    this.token = token;
    this.tableName = tableName;
  }

  public void setTimeout(long timeOut, TimeUnit timeUnit) {
    // proxy API does not support time outs for scanners
    throw ExceptionFactory.unsupported();
  }

  public long getTimeout(TimeUnit timeUnit) {
    // proxy API does not support time outs for scanners
    throw ExceptionFactory.unsupported();
  }

  public void fetchColumnFamily(Text col) {
    fetchColumn(col, null);
  }

  public void fetchColumn(Text colFam, Text colQual) {
    ScanColumn sc = new ScanColumn();
    if (colFam != null) {
      sc.setColFamily(colFam.getBytes());
    }
    if (colQual != null) {
      sc.setColQualifier(colQual.getBytes());
    }
    addToFetchOptions(sc);
  }

  /**
   * Subclasses must set themselves up to fetch the given ScanColumn. This allows Scanners and BatchScanners to handle the ScanColumn option differently.
   * 
   * @param col
   *          the column to add to the current set of fetch options
   */
  protected abstract void addToFetchOptions(ScanColumn col);

}
