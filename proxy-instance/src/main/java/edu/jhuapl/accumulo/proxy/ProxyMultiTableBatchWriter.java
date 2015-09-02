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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.proxy.thrift.UnknownWriter;
import org.apache.accumulo.proxy.thrift.WriterOptions;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a MultiTableBatchWriter for the Proxy Server. The ProxyServer API does not actually expose a MultiTableBatchWriter. Instead, we will "fake"
 * it by managing our own internal list of "regular" ProxyBatchWriters in this class, one for each table requested.
 */
class ProxyMultiTableBatchWriter implements MultiTableBatchWriter {

  ProxyConnector connector;
  ByteBuffer token;
  BatchWriterConfig config;

  MutationBuffer buffer;
  Map<String,MultiProxyBatchWriter> writers;

  ProxyMultiTableBatchWriter(ProxyConnector connector, ByteBuffer token, BatchWriterConfig config) {
    this.connector = connector;
    this.token = token;
    this.config = config;
    writers = new HashMap<String,MultiProxyBatchWriter>();
    buffer = new MutationBuffer(connector, config);
  }

  private void checkClosed() throws IllegalStateException {
    if (isClosed()) {
      throw new IllegalStateException("Closed.");
    }
  }

  public BatchWriter getBatchWriter(String table) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    checkClosed();

    MultiProxyBatchWriter pbw = writers.get(table);

    if (pbw == null) {
      try {
        pbw = new MultiProxyBatchWriter(connector, token, table, buffer);
        writers.put(table, pbw);
      } catch (TException e) {
        throw ExceptionFactory.accumuloException(e);
      }
    }
    return pbw;
  }

  public void flush() throws MutationsRejectedException {
    flushOrClose(false);
  }

  public void close() throws MutationsRejectedException {
    flushOrClose(true);
  }

  private void flushOrClose(boolean close) throws MutationsRejectedException {
    checkClosed();

    buffer.flush();

    List<MutationsRejectedException> exceptions = null;
    try {
      try {
        for (MultiProxyBatchWriter pbw : writers.values()) {
          if (close) {
            pbw.multiClose();
          } else {
            pbw.multiFlush();
          }
        }
      } catch (UnknownWriter e) {
        throw ExceptionFactory.runtimeException(e);
      } catch (org.apache.accumulo.proxy.thrift.MutationsRejectedException e) {
        if (exceptions == null) {
          exceptions = new ArrayList<MutationsRejectedException>();
        }
        exceptions.add(ThriftHelper.fromThrift(e, connector.getInstance()));
      } catch (TException e) {
        throw ExceptionFactory.runtimeException(e);
      }

      writers.clear();
    } finally {
      writers = null;
    }

    checkExceptions(exceptions);
  }

  private void checkExceptions(List<MutationsRejectedException> mres) throws MutationsRejectedException {
    if (mres == null || mres.isEmpty()) {
      return;
    }

    List<ConstraintViolationSummary> cvsList = new LinkedList<ConstraintViolationSummary>();
    HashMap<KeyExtent,Set<SecurityErrorCode>> map = new HashMap<KeyExtent,Set<SecurityErrorCode>>();
    Collection<String> serverSideErrors = new LinkedList<String>();
    int unknownErrors = 0;

    for (MutationsRejectedException mre : mres) {
      cvsList.addAll(mre.getConstraintViolationSummaries());
      map.putAll(mre.getAuthorizationFailuresMap());
      serverSideErrors.addAll(mre.getErrorServers());
      unknownErrors += mre.getUnknownExceptions();
    }

    throw new MutationsRejectedException(null, cvsList, map, serverSideErrors, unknownErrors, null);

  }

  public boolean isClosed() {
    return writers == null;
  }

  @Override
  protected void finalize() {
    if (!isClosed()) {
      LoggerFactory.getLogger(ProxyMultiTableBatchWriter.class).warn(
          "ProxyMultiTableBatchWriter" + " in finalize but not closed; " + "you forgot to close a MultiTableBatchWriter!");

      try {
        close();
      } catch (MutationsRejectedException mre) {
        LoggerFactory.getLogger(ProxyBatchScanner.class).warn("Problem closing ProxyMultiTableBatchWriter.", mre);
      }
    }
  }

  private class MultiProxyBatchWriter implements BatchWriter {

    ProxyConnector connector;
    String writerId;
    MutationBuffer buffer;

    MultiProxyBatchWriter(ProxyConnector connector, ByteBuffer token, String table, MutationBuffer buffer)
        throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
        org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
      this.connector = connector;
      this.buffer = buffer;
      writerId = connector.getClient().createWriter(token, table, new WriterOptions());
    }

    @Override
    public void addMutation(Mutation m) throws MutationsRejectedException {
      checkClosed();

      buffer.addMutation(writerId, m);
    }

    @Override
    public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
      for (Mutation m : iterable) {
        addMutation(m);
      }
    }

    @Override
    public void flush() throws MutationsRejectedException {
      throw new RuntimeException("Cannot flush BatchWriter created by MultiTableBatchWriter directly; " + "flush MultiTableBatchWriter instead.");
    }

    @Override
    public void close() throws MutationsRejectedException {
      throw new RuntimeException("Cannot close BatchWriter created by MultiTableBatchWriter directly; " + "close MultiTableBatchWriter instead.");

    }

    void multiClose() throws UnknownWriter, org.apache.accumulo.proxy.thrift.MutationsRejectedException, TException {
      connector.getClient().closeWriter(writerId);
    }

    void multiFlush() throws UnknownWriter, org.apache.accumulo.proxy.thrift.MutationsRejectedException, TException {
      connector.getClient().flush(writerId);
    }
  }

}
