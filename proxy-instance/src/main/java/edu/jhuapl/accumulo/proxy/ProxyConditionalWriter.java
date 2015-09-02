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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.ConditionalStatus;
import org.apache.accumulo.proxy.thrift.ConditionalUpdates;
import org.apache.accumulo.proxy.thrift.ConditionalWriterOptions;
import org.apache.thrift.TException;

class ProxyConditionalWriter implements ConditionalWriter {

  AccumuloProxy.Iface client;

  String writerId;

  ProxyConditionalWriter(ProxyConnector connector, ByteBuffer token, String table, ConditionalWriterConfig config) {
    this.client = connector.getClient();

    ConditionalWriterOptions options = new ConditionalWriterOptions();
    options.setAuthorizations(new HashSet<ByteBuffer>(config.getAuthorizations().getAuthorizationsBB()));
    options.setThreads(config.getMaxWriteThreads());
    options.setTimeoutMs(config.getTimeout(TimeUnit.MILLISECONDS));
    try {
      writerId = client.createConditionalWriter(token, table, options);
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    }

  }

  public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {
    Map<ByteBuffer,ConditionalUpdates> updates = new HashMap<ByteBuffer,ConditionalUpdates>();
    Map<ByteBuffer,ConditionalMutation> mutationMap = new HashMap<ByteBuffer,ConditionalMutation>();

    while (mutations.hasNext()) {
      ConditionalMutation mutation = mutations.next();
      ByteBuffer key = ByteBuffer.wrap(mutation.getRow());
      ConditionalUpdates update = updates.get(key);

      // NOTE: API seems to imply you will provide a mutation against
      // any single row ID at most once within this iterator or
      // conditional mutations. Otherwise, the mutations and the
      // conditions all get co-mingled when placed in the parameter
      // map! TODO- should we check for this and throw an exception?

      // working under the assumption you do not want to co-mingle updates
      // for a single row within a single call here, we will *replace*
      // (vs. update) anything that was existing in the map. This feels
      // wrong...

      update = new ConditionalUpdates();
      updates.put(key, update);
      mutationMap.put(key, mutation);
      update.setConditions(ThriftHelper.toThriftCondition(mutation.getConditions()));
      List<ColumnUpdate> tupdates = new LinkedList<ColumnUpdate>();
      ThriftHelper.addThriftColumnUpdates(tupdates, mutation.getUpdates());
      update.setUpdates(tupdates);
    }

    try {
      Map<ByteBuffer,ConditionalStatus> status = client.updateRowsConditionally(writerId, updates);

      List<Result> results = new LinkedList<Result>();
      for (Entry<ByteBuffer,ConditionalStatus> entry : status.entrySet()) {
        ConditionalMutation cm = mutationMap.get(entry.getKey());
        Status staus = ThriftHelper.convertEnum(entry.getValue(), Status.class);
        Result r = new Result(staus, cm, null) {

          @Override
          public String getTabletServer() {
            // we are not given the tablet server nor have any way
            // to find out what it is...
            throw ExceptionFactory.unsupported();
          }
        };
        results.add(r);
      }
      return results.iterator();
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

  public Result write(ConditionalMutation mutation) {
    Iterator<Result> results = write(Collections.singleton(mutation).iterator());
    return results.next();
  }

  public void close() {
    try {
      client.closeConditionalWriter(writerId);
    } catch (TException e) {
      throw ExceptionFactory.runtimeException(e);
    }
  }

}
