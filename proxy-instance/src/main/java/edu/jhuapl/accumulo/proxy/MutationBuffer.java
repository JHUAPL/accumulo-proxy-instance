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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.MutationsRejectedException;
import org.apache.thrift.TException;

/**
 * A class used to buffer mutation on the client side in a user-sized memory buffer and/or for a user-defined amount of time before sending to the
 * ProxyInstance. By buffering and sending many requests at once, performance may be improved some situations.
 * 
 * This class does not actually do the sending when thresholds are exceeded; rather it just provides the access to the buffered Mutations and tracks the time
 * and memory in order to be able to inform other classes when the buffer need to be flushed.
 * 
 */
class MutationBuffer {

  /**
   * The map of writerIds to a list of buffered mutations.
   */
  private Map<String,List<Mutation>> mutations;

  /**
   * The maximum amount of memory (in bytes) to use before automatically pushing the buffered mutations to the Proxy server.
   */
  long maxMemory;

  /**
   * The maximum amount of time (in milliseconds) to wait before automatically pushing the buffered mutations to the Proxy server.
   */
  long maxLatencyMs;

  /**
   * The current estimate of buffered mutation memory in bytes.
   */
  long memory;

  ProxyConnector connector;

  Timer timer;

  /**
   * Create a new MutationBuffer with the given configuration parameters.
   * 
   * @param config
   *          the configuration for this buffer
   */
  MutationBuffer(ProxyConnector connector, BatchWriterConfig config) {
    this.connector = connector;
    this.maxMemory = config.getMaxMemory();
    this.maxLatencyMs = config.getMaxLatency(TimeUnit.MILLISECONDS);

    this.memory = 0L;
    this.mutations = new HashMap<String,List<Mutation>>();
  }

  /**
   * Add a mutation to this buffer.
   * 
   * @param mutation
   *          the mutation to add
   * @throws org.apache.accumulo.core.client.MutationsRejectedException
   *           throws if the mutation cannot be accepted
   */
  public synchronized void addMutation(String writerId, Mutation mutation) throws org.apache.accumulo.core.client.MutationsRejectedException {
    if (mutations.isEmpty()) {
      // this is the first entry... start watching latency...
      timer = new Timer("", true);
      timer.schedule(new TimerTask() {

        @Override
        public void run() {
          try {
            latencyFlush();
          } catch (org.apache.accumulo.core.client.MutationsRejectedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }

      }, maxLatencyMs);
    }

    // create our own copy...
    mutation = new Mutation(mutation);

    List<Mutation> list = mutations.get(writerId);
    if (list == null) {
      list = new ArrayList<Mutation>();
      mutations.put(writerId, list);
    }
    list.add(mutation);

    memory += mutation.estimatedMemoryUsed();
    checkMemoryFlush();
  }

  void checkMemoryFlush() throws org.apache.accumulo.core.client.MutationsRejectedException {
    if (memory >= maxMemory) {
      // memory threshold exceeded
      flush();
    }
  }

  void latencyFlush() throws org.apache.accumulo.core.client.MutationsRejectedException {
    if (mutations.size() != 0) {
      // latency threshold exceeded
      flush();
    }
  }

  /**
   * Called to flush the buffer. The method returns a map of rowIDs (as ByteBuffers) to lists of Thrift-based ColumnUpdates suitable for sending directly to the
   * ProxyInstance. As a side effect, this method also resets this buffer.
   * 
   * @throws org.apache.accumulo.core.client.MutationsRejectedException
   *           thrown if any buffered mutations are rejected while flushing
   */
  public synchronized void flush() throws org.apache.accumulo.core.client.MutationsRejectedException {
    for (Entry<String,List<Mutation>> entry : mutations.entrySet()) {
      String writerId = entry.getKey();
      Map<ByteBuffer,List<ColumnUpdate>> updates = new HashMap<ByteBuffer,List<ColumnUpdate>>();
      for (Mutation m : entry.getValue()) {
        ByteBuffer key = ByteBuffer.wrap(m.getRow());
        List<ColumnUpdate> updateList = updates.get(key);
        if (updateList == null) {
          updateList = new ArrayList<ColumnUpdate>();
          updates.put(key, updateList);
        }
        ThriftHelper.addThriftColumnUpdates(updateList, m.getUpdates());
      }
      try {
        connector.getClient().update(writerId, updates);
        connector.getClient().flush(writerId);
      } catch (MutationsRejectedException mre) {
        throw ThriftHelper.fromThrift(mre, connector.getInstance());
      } catch (TException e) {
        throw ExceptionFactory.runtimeException(e);
      }
    }

    // and reset...
    memory = 0;
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
    mutations.clear();
  }

}
