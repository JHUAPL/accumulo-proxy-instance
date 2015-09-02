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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class ThiftHelperTest {

  /**
   * Used for enum mapping testing
   */
  public enum FirstEnum {
    val1, val2
  };

  /**
   * Used for enum mapping testing
   */
  public enum SecondEnum {
    val1
  };

  @Test
  public void testToText() {
    String test = "test";

    Text txt = ThriftHelper.toText(test);
    Assert.assertEquals(test, txt.toString());

    txt = ThriftHelper.toText(test.getBytes());
    Assert.assertEquals(test, txt.toString());

    txt = ThriftHelper.toText((byte[]) null);
    Assert.assertNull(txt);
  }

  @Test
  public void testEnum() {
    SecondEnum se = ThriftHelper.convertEnum(null, SecondEnum.class);
    Assert.assertNull(se);

    se = ThriftHelper.convertEnum(FirstEnum.val1, SecondEnum.class);
    Assert.assertEquals(SecondEnum.val1, se);
  }

  @Test(expected = ProxyInstanceError.class)
  public void testEnumBadMapping() {
    ThriftHelper.convertEnum(FirstEnum.val2, SecondEnum.class);
  }

  @Test
  public void testToByteBuffer() {
    ByteBuffer buf = ThriftHelper.toByteBuffer(null);
    Assert.assertNull(buf);

    Text text = new Text("test test");
    buf = ThriftHelper.toByteBuffer(text);
    Assert.assertArrayEquals(text.getBytes(), buf.array());
  }

  @Test
  public void testRanges() {
    testConvertRange(new Range("a", "b"));
    testConvertRange(new Range(null, "b"));
    testConvertRange(new Range("a", null));
    testConvertRange(null);
    testConvertRange(new Range("a", true, "b", false));
    testConvertRange(new Range("a", false, "b", false));
    testConvertRange(new Range("a", false, "b", true));
  }

  private void testConvertRange(Range range) {
    org.apache.accumulo.proxy.thrift.Range r2 = ThriftHelper.toThrift(range);
    checkRanges(range, r2);

    range = ThriftHelper.fromThrift(r2);
    checkRanges(range, r2);
  }

  private void checkRanges(Range r1, org.apache.accumulo.proxy.thrift.Range r2) {
    if (r1 == null) {
      Assert.assertNull("t1 was null, but r2 was not!", r2);
      return;
    } else if (r2 == null) {
      Assert.fail("r1 was not null, but r2 was!");
      return;
    }

    checkKeys(r1.getStartKey(), r2.getStart());
    checkKeys(r1.getEndKey(), r2.getStop());
    Assert.assertEquals(r1.isStartKeyInclusive(), r2.isStartInclusive());
    Assert.assertEquals(r1.isEndKeyInclusive(), r2.isStopInclusive());
  }

  private void checkKeys(Key k, org.apache.accumulo.proxy.thrift.Key k2) {
    if (k == null) {
      Assert.assertNull("k was null, but k2 was not!", k2);
      return;
    } else if (k2 == null) {
      Assert.fail("k was not null, but k2 was!");
      return;
    }
    Assert.assertArrayEquals(k.getRow().getBytes(), k2.getRow());
    Assert.assertArrayEquals(k.getColumnFamily().getBytes(), k2.getColFamily());
    Assert.assertArrayEquals(k.getColumnQualifier().getBytes(), k2.getColQualifier());
    Assert.assertArrayEquals(k.getColumnVisibility().getBytes(), k2.getColVisibility());
    Assert.assertEquals(k.getTimestamp(), k2.getTimestamp());
  }

}
