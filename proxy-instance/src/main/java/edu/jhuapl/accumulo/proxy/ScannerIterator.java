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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.proxy.thrift.ScanResult;
import org.apache.thrift.TException;

/**
 * Givens a scanner id and connection to a proxy server, creates an iterator for the KV provided for the scanner id from the proxy.
 */
class ScannerIterator implements Iterator<Entry<Key,Value>> {

  String id;
  ProxyConnector connector;
  int fetchSize;

  private boolean hasMore;
  private Iterator<org.apache.accumulo.proxy.thrift.KeyValue> results;

  ScannerIterator(String id, ProxyConnector connector, int fetchSize) {
    this.id = id;
    this.connector = connector;
    this.fetchSize = fetchSize;

    // setup to make the class look for "more" data on 1st pass...
    this.results = Collections.emptyListIterator();
    this.hasMore = true;
  }

  public boolean hasNext() {
    try {
      while (!results.hasNext()) {
        // either first pass or we are at the end of the current batch.
        if (!hasMore) {
          // last check told us we were done at the end of this;
          // hasMore is initialized to true to ensure this does not
          // fail on 1st call
          return false;
        }

        ScanResult sr = connector.getClient().nextK(id, fetchSize);
        results = sr.getResultsIterator();
        hasMore = sr.isMore();

        // We cannot return true from this method unless there is at
        // least one result in the results list. There is a possibility
        // that the results are empty now, but hasMore is still true.
        // Therefore, we iterate until either hasMore is false (we are
        // done!) or results.hasNext() is true
      }

      // if we got here, there is something in results
      return true;
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

  public Entry<Key,Value> next() {
    if (!hasNext()) {
      // need to call hasNext to make sure results is setup correctly;
      // might as well use the return to throw our own exception here
      // instead of waiting for the call to results.next() to generate
      // it...
      throw new NoSuchElementException();
    }

    org.apache.accumulo.proxy.thrift.KeyValue kv = results.next();
    org.apache.accumulo.proxy.thrift.Key k = kv.getKey();

    Key key = new Key(k.getRow(), k.getColFamily(), k.getColQualifier(), k.getColVisibility(), k.getTimestamp());
    return new KeyValue(key, kv.getValue());
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}
