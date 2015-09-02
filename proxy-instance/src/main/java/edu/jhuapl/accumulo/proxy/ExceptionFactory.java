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
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

/**
 * A factory to create standard exceptions for specific conditions.
 * */
final class ExceptionFactory {

  /**
   * As a utility, this should not be instantiated.
   */
  private ExceptionFactory() {}

  /**
   * Exception to throw when a Java Client API method is not supported in the Proxy Client API.
   * 
   * @return a new UnsupportedOperationException that tells the user this feature is not supported by the Proxy Thrift interface.
   */
  public static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Operation not supported by Accumulo Proxy API.");
  }

  /**
   * Exception to throw when the ProxyInstace has not yet implemented a Java API method.
   * 
   * @return a new UnsupportedOperationException that tells the user this feature is not implemented yet.
   */

  public static UnsupportedOperationException notYetImplemented() {
    return new UnsupportedOperationException("Not yet implemented.");
  }

  /**
   * Creates an AccumuloException with the given Throwable embedded as the cause.
   * 
   * @param cause
   *          the root cause of this exception
   * @return a new AccumuloException with the given root cause
   */
  public static AccumuloException accumuloException(Throwable cause) {
    return new AccumuloException(cause);
  }

  /**
   * Wraps the thrift exception with a core client exception.
   * 
   * @param tableName
   *          the table name that was not found.
   * @param tnfe
   *          the Thrift TableNotFoundException
   * @return a new (non-Thrift) TableNotFoundException
   */
  public static TableNotFoundException tableNotFoundException(String tableName, org.apache.accumulo.proxy.thrift.TableNotFoundException tnfe) {
    return new TableNotFoundException(null, tableName, tnfe.getMsg(), tnfe);
  }

  /**
   * Wraps the thrift exception with a core client exception.
   * 
   * @param tableName
   *          the table name that already exists
   * @param tee
   *          the Thrift TableExistsException
   * @return a new (non-Thrift) TableExistsException
   */
  public static TableExistsException tableExistsException(String tableName, org.apache.accumulo.proxy.thrift.TableExistsException tee) {
    return new TableExistsException(null, tableName, tee.getMsg(), tee);
  }

  /**
   * Creates a new RuntimeException with the given Throwable as the cause.
   * 
   * @param cause
   *          the cause of this exception
   * @return a new RuntimeException with the given cause
   */
  public static RuntimeException runtimeException(Throwable cause) {
    return new RuntimeException(cause);
  }

  /**
   * Used when we expect to be able to convert a particular value from one enum type (e.g., Java API) in another enum type (e.g., Proxy API) but can't. As we
   * expect this to always work, this is an Error, not an Exception.
   * 
   * @param val
   *          the original enumeration value
   * @param otherCls
   *          the other Enumeration type we were trying to which we were trying to map {@code val}
   * @return a new ProxyInstanceError that informs the user we expected but were unable to find a valid enumeration mapping.
   */
  public static ProxyInstanceError noEnumMapping(Enum<?> val, Class<? extends Enum<?>> otherCls) {
    return new ProxyInstanceError("Expected mapping from value " + val + " (" + val.getClass().getCanonicalName() + ") to " + otherCls.getCanonicalName()
        + " but it does not exist.");
  }
}
