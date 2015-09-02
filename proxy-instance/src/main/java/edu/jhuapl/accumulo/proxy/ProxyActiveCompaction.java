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

import java.util.List;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.data.KeyExtent;

/**
 * Implementation of an ActiveCompaction for the ProxyInstance. This is basically a data structure to hold all of the provided details about an active
 * compaction.
 */
class ProxyActiveCompaction extends ActiveCompaction {

  String table;
  KeyExtent extent;
  long age;
  List<String> inputFiles;
  String outputFile;
  CompactionType type;
  CompactionReason reason;
  String localityGroup;
  long entriesRead;
  long entriesWritten;
  List<IteratorSetting> iterators;

  ProxyActiveCompaction(String table, KeyExtent extent, long age, List<String> inputFiles, String outputFile, CompactionType type, CompactionReason reason,
      String localityGroup, long entriesRead, long entriesWritten, List<IteratorSetting> iterators) {
    super();
    this.table = table;
    this.extent = extent;
    this.age = age;
    this.inputFiles = inputFiles;
    this.outputFile = outputFile;
    this.type = type;
    this.reason = reason;
    this.localityGroup = localityGroup;
    this.entriesRead = entriesRead;
    this.entriesWritten = entriesWritten;
    this.iterators = iterators;
  }

  @Override
  public String getTable() throws TableNotFoundException {
    return table;
  }

  @Override
  public KeyExtent getExtent() {
    return extent;
  }

  @Override
  public long getAge() {
    return age;
  }

  @Override
  public List<String> getInputFiles() {
    return inputFiles;
  }

  @Override
  public String getOutputFile() {
    return outputFile;
  }

  @Override
  public CompactionType getType() {
    return type;
  }

  @Override
  public CompactionReason getReason() {
    return reason;
  }

  @Override
  public String getLocalityGroup() {
    return localityGroup;
  }

  @Override
  public long getEntriesRead() {
    return entriesRead;
  }

  @Override
  public long getEntriesWritten() {
    return entriesWritten;
  }

  @Override
  public List<IteratorSetting> getIterators() {
    return iterators;
  }

  @Override
  public String toString() {
    return "ProxyActiveCompaction [table=" + table + ", extent=" + extent + ", age=" + age + ", inputFiles=" + inputFiles + ", outputFile=" + outputFile
        + ", type=" + type + ", reason=" + reason + ", localityGroup=" + localityGroup + ", entriesRead=" + entriesRead + ", entriesWritten=" + entriesWritten
        + ", iterators=" + iterators + "]";
  }

}
