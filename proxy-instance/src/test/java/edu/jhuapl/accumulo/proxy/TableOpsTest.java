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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.junit.Assert;
import org.junit.Test;

public class TableOpsTest extends ConnectorBase {

  String table = "__TEST_TABLE__";

  @Test
  public void testTableOps() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    TableOperations tops = connector.tableOperations();

    Assert.assertFalse(tops.exists(table));
    tops.create(table);
    Assert.assertTrue(tops.exists(table));
    tops.delete(table);
    Assert.assertFalse(tops.exists(table));
  }

  @Test
  public void testCreateExistingTable() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
    TableOperations tops = connector.tableOperations();
    try {
      Assert.assertFalse(tops.exists(table));
      tops.create(table);
      Assert.assertTrue(tops.exists(table));

      try {
        tops.create(table);
        Assert.fail("Expected second table create to fail.");
      } catch (TableExistsException tee) {
        // expected
        Assert.assertTrue(true);
      }
    } finally {
      if (tops.exists(table)) {
        tops.delete(table);
      }
      Assert.assertFalse(tops.exists(table));
    }
  }

  @Test
  public void testDeleteNonExistentTable() throws AccumuloException, AccumuloSecurityException {
    TableOperations tops = connector.tableOperations();

    Assert.assertFalse(tops.exists(table));
    try {
      tops.delete(table);
      Assert.fail("Expected second table create to fail.");
    } catch (TableNotFoundException tnfe) {
      // expected
      Assert.assertTrue(true);
    }
  }

}
