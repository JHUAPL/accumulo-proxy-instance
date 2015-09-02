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

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.proxy.thrift.AccumuloException;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.proxy.thrift.AccumuloSecurityException;
import org.apache.accumulo.proxy.thrift.TableNotFoundException;
import org.apache.accumulo.proxy.thrift.UnknownWriter;
import org.apache.accumulo.proxy.thrift.WriterOptions;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a BatchWriter to a Proxy Server.
 */
class ProxyBatchWriter implements BatchWriter {

  /**
   * Parent connector.
   */
  ProxyConnector connector;

  /**
   * Local copy of connector's Thrift RPC object to Proxy Server.
   */
  private AccumuloProxy.Iface client;

  /**
   * Token used when communicating with the proxy server.
   */
  ByteBuffer token;

  /**
   * The id for this writer.
   */
  String writerId;

  /**
   * An internal buffer to provide in-memory, time-based buffering of mutations to send in batches. This uses the same memory and latency parameters the actual
   * BatchWriter will use on the Proxy Server side as well.
   */
  private MutationBuffer mutationBuffer;

  private boolean closed;

  ProxyBatchWriter(ProxyConnector connector, ByteBuffer token, String table, BatchWriterConfig config) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, TException {
    this.connector = connector;
    this.client = connector.getClient();

    this.token = token;
    WriterOptions opts = new WriterOptions();
    opts.setLatencyMs(config.getMaxLatency(TimeUnit.MILLISECONDS));
    opts.setMaxMemory(config.getMaxMemory());
    opts.setThreads(config.getMaxWriteThreads());
    writerId = client.createWriter(token, table, opts);

    mutationBuffer = new MutationBuffer(connector, config);
    closed = false;
  }

  private void checkClosed() throws IllegalStateException {
    if (closed) {
      throw new IllegalStateException("Closed.");
    }
  }

  @Override
  public void addMutation(Mutation m) throws MutationsRejectedException {
    checkClosed();
    addMutationNoCloseCheck(m);
  }

  @Override
  public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException {
    checkClosed();
    for (Mutation m : iterable) {
      addMutationNoCloseCheck(m);
    }
  }

  private void addMutationNoCloseCheck(Mutation m) throws MutationsRejectedException {
    if (m.size() == 0) {
      throw new IllegalArgumentException("Cannot add empty mutation.");
    }

    mutationBuffer.addMutation(writerId, m);
  }

  @Override
  public void flush() throws MutationsRejectedException {
    checkClosed();
    mutationBuffer.flush();
  }

  @Override
  public void close() throws MutationsRejectedException {
    checkClosed();
    mutationBuffer.flush();

    try {
      client.closeWriter(writerId);
      closed = true;
    } catch (UnknownWriter e) {
      throw ExceptionFactory.runtimeException(e);
    } catch (org.apache.accumulo.proxy.thrift.MutationsRejectedException e) {
      throw ThriftHelper.fromThrift(e, connector.getInstance());
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

  @Override
  protected void finalize() {
    if (!closed) {
      LoggerFactory.getLogger(ProxyBatchWriter.class).warn(
          "ProxyBatchWriter ID " + writerId + " in finalize but not closed; " + "you forgot to close a ProxyBatchWriter!");

      try {
        close();
      } catch (MutationsRejectedException mre) {
        LoggerFactory.getLogger(ProxyBatchScanner.class).warn("Problem closing ProxyMultiTableBatchWriter.", mre);
      }
    }
  }

}
