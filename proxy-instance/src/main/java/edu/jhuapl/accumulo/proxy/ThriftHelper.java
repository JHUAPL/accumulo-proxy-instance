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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.ActiveCompaction.CompactionReason;
import org.apache.accumulo.core.client.admin.ActiveCompaction.CompactionType;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.ScanState;
import org.apache.accumulo.core.client.admin.ScanType;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.io.Text;

/**
 * Utility class to translate between Accumulo<->Thrift objects.
 * 
 */
final class ThriftHelper {

  /**
   * UTF-8 charset that can be used to convert String to/from byte arrays.
   */
  static final Charset UTF8 = Charset.forName("UTF-8");

  /**
   * As a utility should not be instantiated.
   */
  private ThriftHelper() {

  }

  public static Map<String,EnumSet<IteratorScope>> fromThrift(Map<String,Set<org.apache.accumulo.proxy.thrift.IteratorScope>> tmap) {
    if (tmap == null) {
      return null;
    }

    Map<String,EnumSet<IteratorScope>> map = new HashMap<String,EnumSet<IteratorScope>>();

    for (Entry<String,Set<org.apache.accumulo.proxy.thrift.IteratorScope>> entry : tmap.entrySet()) {
      map.put(entry.getKey(), fromThrift(entry.getValue()));
    }
    return map;
  }

  public static Set<org.apache.accumulo.proxy.thrift.IteratorScope> toThrift(EnumSet<IteratorScope> scopes) {
    if (scopes == null) {
      return null;
    }

    HashSet<org.apache.accumulo.proxy.thrift.IteratorScope> set = new HashSet<org.apache.accumulo.proxy.thrift.IteratorScope>();
    for (IteratorScope is : scopes) {
      set.add(toThrift(is));
    }
    return set;
  }

  public static EnumSet<IteratorScope> fromThrift(Set<org.apache.accumulo.proxy.thrift.IteratorScope> tscopes) {
    if (tscopes == null) {
      return null;
    }

    LinkedList<IteratorScope> list = new LinkedList<IteratorScope>();
    for (org.apache.accumulo.proxy.thrift.IteratorScope tis : tscopes) {
      list.add(fromThrift(tis));
    }

    return EnumSet.copyOf(list);
  }

  public static org.apache.accumulo.proxy.thrift.IteratorScope toThrift(IteratorScope is) {
    // we can't just use the enum values, since thrift MINC=0 but Apache
    // minc=1. we can't just use the names, since thrift are in upper case,
    // and/ Apache are lower. assuming Thrift will remain uppercase and use
    // the same characters as Apache
    try {
      return org.apache.accumulo.proxy.thrift.IteratorScope.valueOf(is.name().toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw ExceptionFactory.noEnumMapping(is, org.apache.accumulo.proxy.thrift.IteratorScope.class);
    }
  }

  public static IteratorScope fromThrift(org.apache.accumulo.proxy.thrift.IteratorScope tis) {
    if (tis == null) {
      return null;
    }

    // we can't just use the enum values, since thrift MINC=0 but Apache
    // minc=1. we can't just use the names, since thrift are in upper case,
    // and Apache are lower. assuming Thrift will remain uppercase and use
    // the same characters as Apache
    try {
      return IteratorScope.valueOf(tis.name().toLowerCase());
    } catch (IllegalArgumentException ex) {
      throw ExceptionFactory.noEnumMapping(tis, IteratorScope.class);
    }
  }

  public static org.apache.accumulo.proxy.thrift.IteratorSetting toThrift(IteratorSetting is) {
    if (is == null) {
      return null;
    }

    org.apache.accumulo.proxy.thrift.IteratorSetting tis = new org.apache.accumulo.proxy.thrift.IteratorSetting();
    tis.setIteratorClass(is.getIteratorClass());
    tis.setName(is.getName());
    tis.setPriority(is.getPriority());
    tis.setProperties(is.getOptions());
    return tis;
  }

  public static org.apache.accumulo.proxy.thrift.Key toThrift(Key key) {
    if (key == null) {
      return null;
    }

    org.apache.accumulo.proxy.thrift.Key tkey = new org.apache.accumulo.proxy.thrift.Key();
    tkey.setRow(key.getRow().getBytes());
    tkey.setColFamily(key.getColumnFamily().getBytes());
    tkey.setColQualifier(key.getColumnQualifier().getBytes());
    tkey.setColVisibility(key.getColumnVisibility().getBytes());
    tkey.setTimestamp(key.getTimestamp());
    return tkey;
  }

  public static Key fromThrift(org.apache.accumulo.proxy.thrift.Key tkey) {
    if (tkey == null) {
      return null;
    }

    return new Key(tkey.getRow(), tkey.getColFamily(), tkey.getColQualifier(), tkey.getColVisibility(), tkey.getTimestamp());
  }

  public static List<org.apache.accumulo.proxy.thrift.Range> toThriftRanges(Collection<Range> ranges) {
    if (ranges == null) {
      return null;
    }

    List<org.apache.accumulo.proxy.thrift.Range> tranges = new ArrayList<org.apache.accumulo.proxy.thrift.Range>(ranges.size());
    for (Range range : ranges) {
      tranges.add(toThrift(range));
    }
    return tranges;
  }

  public static org.apache.accumulo.proxy.thrift.Range toThrift(Range range) {
    if (range == null) {
      return null;
    }

    org.apache.accumulo.proxy.thrift.Range trange = new org.apache.accumulo.proxy.thrift.Range();
    trange.setStart(toThrift(range.getStartKey()));
    trange.setStop(toThrift(range.getEndKey()));
    trange.setStartInclusive(range.isStartKeyInclusive());
    trange.setStopInclusive(range.isEndKeyInclusive());
    return trange;
  }

  public static Set<Range> fromThriftRanges(Set<org.apache.accumulo.proxy.thrift.Range> tranges) {
    if (tranges == null) {
      return null;
    }

    Set<Range> ranges = new HashSet<Range>();
    for (org.apache.accumulo.proxy.thrift.Range trange : tranges) {
      ranges.add(fromThrift(trange));
    }
    return ranges;
  }

  public static Range fromThrift(org.apache.accumulo.proxy.thrift.Range trange) {
    if (trange == null) {
      return null;
    }

    return new Range(fromThrift(trange.getStart()), trange.isStartInclusive(), fromThrift(trange.getStop()), trange.isStopInclusive());
  }

  /**
   * Converts each non-thrift ColumnUpdate in {@code list} to a Thrift ColumnUpdate (see {@link #toThrift(ColumnUpdate)}) and adds it to the provided
   * {@code thriftUpdates} list. If {@code updates} is null or empty, {@code thriftUpdates} will not be changed.
   * 
   * @param thriftUpdates
   *          the list where the newly created Thrift ColumnUpdates will be added.
   * @param updates
   *          the list of ColumnUpdates to be converted to Thrift ColumnUpdates
   */
  public static void addThriftColumnUpdates(List<org.apache.accumulo.proxy.thrift.ColumnUpdate> thriftUpdates, List<ColumnUpdate> updates) {
    if (updates == null) {
      return;
    }

    for (ColumnUpdate cu : updates) {
      thriftUpdates.add(toThrift(cu));
    }
  }

  public static org.apache.accumulo.proxy.thrift.ColumnUpdate toThrift(ColumnUpdate update) {
    if (update == null) {
      return null;
    }

    org.apache.accumulo.proxy.thrift.ColumnUpdate tcu = new org.apache.accumulo.proxy.thrift.ColumnUpdate();
    tcu.setColFamily(update.getColumnFamily());
    tcu.setColQualifier(update.getColumnQualifier());
    tcu.setColVisibility(update.getColumnVisibility());
    tcu.setTimestamp(update.getTimestamp());
    tcu.setValue(update.getValue());
    // Work around for ACCUMULO-3474
    if (update.isDeleted()) {
      tcu.setDeleteCell(update.isDeleted());
    }
    return tcu;
  }

  public static List<ActiveScan> fromThriftActiveScans(List<org.apache.accumulo.proxy.thrift.ActiveScan> tscans) {
    if (tscans == null) {
      return null;
    }

    List<ActiveScan> scans = new ArrayList<ActiveScan>(tscans.size());
    for (org.apache.accumulo.proxy.thrift.ActiveScan tscan : tscans) {
      scans.add(fromThrift(tscan));
    }
    return scans;
  }

  public static ActiveScan fromThrift(org.apache.accumulo.proxy.thrift.ActiveScan scan) {
    if (scan == null) {
      return null;
    }

    // Note- scanId is not provided by the server and does not appear to be
    // set/used in actual implementation... we will just set it to 0.
    return new ProxyActiveScan(0, scan.getClient(), scan.getUser(), scan.getTable(), scan.getAge(), System.currentTimeMillis() - scan.getIdleTime(),
        convertEnum(scan.getType(), ScanType.class), convertEnum(scan.getState(), ScanState.class), fromThrift(scan.getExtent()),
        fromThriftColumns(scan.getColumns()), scan.getAuthorizations(), scan.getIdleTime());

  }

  public static KeyExtent fromThrift(org.apache.accumulo.proxy.thrift.KeyExtent textent) {
    if (textent == null) {
      return null;
    }

    return new KeyExtent(toText(textent.getTableId()), toText(textent.getEndRow()), toText(textent.getPrevEndRow()));
  }

  public static List<Column> fromThriftColumns(List<org.apache.accumulo.proxy.thrift.Column> tcolumns) {
    if (tcolumns == null) {
      return null;
    }

    List<Column> columns = new ArrayList<Column>(tcolumns.size());
    for (org.apache.accumulo.proxy.thrift.Column tcolumn : tcolumns) {
      columns.add(fromThrift(tcolumn));
    }
    return columns;
  }

  public static Column fromThrift(org.apache.accumulo.proxy.thrift.Column tcolumn) {
    if (tcolumn == null) {
      return null;
    }

    return new Column(tcolumn.getColFamily(), tcolumn.getColQualifier(), tcolumn.getColVisibility());
  }

  public static List<ActiveCompaction> fromThriftActiveCompactions(Map<String,String> tableIdMap,
      List<org.apache.accumulo.proxy.thrift.ActiveCompaction> tcompactions) {
    if (tcompactions == null) {
      return null;
    }

    List<ActiveCompaction> compactions = new ArrayList<ActiveCompaction>(tcompactions.size());
    for (org.apache.accumulo.proxy.thrift.ActiveCompaction tcompaction : tcompactions) {
      compactions.add(fromThrift(getTableNameFromId(tableIdMap, tcompaction.getExtent().getTableId()), tcompaction));
    }
    return compactions;
  }

  public static ActiveCompaction fromThrift(String tableName, org.apache.accumulo.proxy.thrift.ActiveCompaction tcompaction) {
    if (tcompaction == null) {
      return null;
    }

    return new ProxyActiveCompaction(tableName, fromThrift(tcompaction.getExtent()), tcompaction.getAge(), tcompaction.getInputFiles(),
        tcompaction.getOutputFile(), convertEnum(tcompaction.getType(), CompactionType.class), convertEnum(tcompaction.getReason(), CompactionReason.class),
        tcompaction.getLocalityGroup(), tcompaction.getEntriesRead(), tcompaction.getEntriesWritten(), fromThriftIteratorSettings(tcompaction.getIterators()));
  }

  public static String getTableNameFromId(Map<String,String> tableIdMap, String id) {
    if (id == null) {
      return null;
    }

    for (Entry<String,String> entry : tableIdMap.entrySet()) {
      if (id.equalsIgnoreCase(entry.getKey())) {
        return entry.getValue();
      }
    }
    return null;
  }

  public static List<IteratorSetting> fromThriftIteratorSettings(List<org.apache.accumulo.proxy.thrift.IteratorSetting> tsettings) {
    if (tsettings == null) {
      return null;
    }

    List<IteratorSetting> settings = new ArrayList<IteratorSetting>(tsettings.size());
    for (org.apache.accumulo.proxy.thrift.IteratorSetting tsetting : tsettings) {
      settings.add(fromThrift(tsetting));
    }
    return settings;
  }

  public static IteratorSetting fromThrift(org.apache.accumulo.proxy.thrift.IteratorSetting tsetting) {
    if (tsetting == null) {
      return null;
    }

    return new IteratorSetting(tsetting.getPriority(), tsetting.getName(), tsetting.getIteratorClass(), tsetting.getProperties());
  }

  public static List<org.apache.accumulo.proxy.thrift.IteratorSetting> toThriftIteratorSettings(List<IteratorSetting> settings) {
    if (settings == null) {
      return null;
    }

    List<org.apache.accumulo.proxy.thrift.IteratorSetting> tsettings = new ArrayList<org.apache.accumulo.proxy.thrift.IteratorSetting>(settings.size());
    for (IteratorSetting setting : settings) {
      tsettings.add(toThrift(setting));
    }
    return tsettings;
  }

  public static List<DiskUsage> fromThriftDiskUsage(List<org.apache.accumulo.proxy.thrift.DiskUsage> tusage) {
    if (tusage == null) {
      return null;
    }

    List<DiskUsage> usage = new ArrayList<DiskUsage>(tusage.size());
    for (org.apache.accumulo.proxy.thrift.DiskUsage tdu : tusage) {
      usage.add(fromThrift(tdu));
    }
    return usage;
  }

  public static DiskUsage fromThrift(org.apache.accumulo.proxy.thrift.DiskUsage tusage) {
    if (tusage == null) {
      return null;
    }

    return new DiskUsage(new TreeSet<String>(tusage.getTables()), tusage.getUsage());
  }

  public static List<org.apache.accumulo.proxy.thrift.Condition> toThriftCondition(List<Condition> conditions) {
    if (conditions == null) {
      return null;
    }

    List<org.apache.accumulo.proxy.thrift.Condition> tconditions = new ArrayList<org.apache.accumulo.proxy.thrift.Condition>(conditions.size());
    for (Condition c : conditions) {
      tconditions.add(toThrift(c));
    }
    return tconditions;
  }

  public static org.apache.accumulo.proxy.thrift.Condition toThrift(Condition condition) {
    if (condition == null) {
      return null;
    }

    org.apache.accumulo.proxy.thrift.Condition tcondition = new org.apache.accumulo.proxy.thrift.Condition();

    org.apache.accumulo.proxy.thrift.Column col = new org.apache.accumulo.proxy.thrift.Column();
    if (condition.getFamily() != null) {
      col.setColFamily(condition.getFamily().getBackingArray());
    }
    if (condition.getQualifier() != null) {
      col.setColQualifier(condition.getQualifier().getBackingArray());
    }

    if (col.getColVisibility() != null) {
      col.setColVisibility(condition.getQualifier().getBackingArray());
    }
    tcondition.setColumn(col);

    if (condition.getTimestamp() != null) {
      tcondition.setTimestamp(condition.getTimestamp());
    }

    if (condition.getValue() != null) {
      tcondition.setValue(condition.getValue().getBackingArray());
    }
    tcondition.setIterators(toThriftIteratorSettings(Arrays.asList(condition.getIterators())));
    return tcondition;
  }

  public static MutationsRejectedException fromThrift(org.apache.accumulo.proxy.thrift.MutationsRejectedException mre, Instance instance) {
    return new MutationsRejectedException(instance, null, null, Collections.singleton(mre.getMsg()), 0, mre);
  }

  /**
   * Converts the enumeration of class F to the enumeration of class S assuming both F and S define the same values. This is used in mapping between Accumulo
   * client-side enumerations and their equivalent Thrift-based enumerations.
   * 
   * @param first
   *          the enumeration to convert
   * @param cls
   *          the new class of the enumeration
   * @return an enumeration of type <code>cls</code> with the same name as <code>first</code>.
   * @exception ProxyInstanceError
   *              thrown if the text of <code>first</code> is not a valid enumerated value in the class <code>cls</code>.
   */
  public static <F extends Enum<F>,S extends Enum<S>> S convertEnum(F first, Class<S> cls) throws ProxyInstanceError {
    if (first == null) {
      return null;
    }

    try {
      return Enum.valueOf(cls, first.toString());
    } catch (IllegalArgumentException e) {
      throw ExceptionFactory.noEnumMapping(first, cls);
    }
  }

  public static Text toText(String val) {
    if (val == null) {
      return null;
    } else {
      return new Text(val.getBytes(UTF8));
    }
  }

  public static Text toText(byte[] val) {
    if (val == null) {
      return null;
    } else {
      return new Text(val);
    }
  }

  public static ByteBuffer toByteBuffer(Text val) {
    if (val == null) {
      return null;
    } else {
      return ByteBuffer.wrap(val.getBytes());
    }
  }
}
