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

import static edu.jhuapl.accumulo.proxy.ThriftHelper.UTF8;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.thrift.TException;

/**
 * Implements SecurityOperations as a pass through to a proxy server.
 */

class ProxySecurityOperations implements SecurityOperations {

  AccumuloProxy.Iface client;
  ByteBuffer token;

  ProxySecurityOperations(ProxyConnector connector, ByteBuffer token) {
    this.client = connector.getClient();
    this.token = token;
  }

  @Deprecated
  public void createUser(String user, byte[] password, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    createLocalUser(user, new PasswordToken(password));
    changeUserAuthorizations(user, authorizations);
  }

  public void createLocalUser(String principal, PasswordToken password) throws AccumuloException, AccumuloSecurityException {
    try {
      client.createLocalUser(token, principal, ByteBuffer.wrap(password.getPassword()));
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  @Deprecated
  public void dropUser(String user) throws AccumuloException, AccumuloSecurityException {
    dropLocalUser(user);
  }

  public void dropLocalUser(String principal) throws AccumuloException, AccumuloSecurityException {
    try {
      client.dropLocalUser(token, principal);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  @Deprecated
  public boolean authenticateUser(String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
    return this.authenticateUser(user, new PasswordToken(password));
  }

  public boolean authenticateUser(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {

    if (!(token instanceof PasswordToken)) {
      throw ExceptionFactory.notYetImplemented();
    }
    PasswordToken passwd = (PasswordToken) token;
    try {
      Map<String,String> properties = new HashMap<String,String>();
      properties.put("password", new String(passwd.getPassword(), UTF8));
      return client.authenticateUser(this.token, principal, properties);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  @Deprecated
  public void changeUserPassword(String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
    changeLocalUserPassword(user, new PasswordToken(password));
  }

  public void changeLocalUserPassword(String principal, PasswordToken token) throws AccumuloException, AccumuloSecurityException {
    try {
      client.changeLocalUserPassword(this.token, principal, ByteBuffer.wrap(token.getPassword()));
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void changeUserAuthorizations(String principal, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    try {
      client.changeUserAuthorizations(token, principal, new HashSet<ByteBuffer>(authorizations.getAuthorizationsBB()));
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public Authorizations getUserAuthorizations(String principal) throws AccumuloException, AccumuloSecurityException {
    try {
      return new Authorizations(client.getUserAuthorizations(token, principal));
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  /**
   * ProxyAPI specifies AccumuloException and AccumuloSecurityException may be thrown. The IllegalArgumentException may be thrown if the
   * {@link ThriftHelper#convertEnum(Enum, Class)} fails because the Java and Thrift SystemPermission no longer match; this should never happen.
   */
  public boolean hasSystemPermission(String principal, SystemPermission perm) throws AccumuloException, AccumuloSecurityException, InternalError {
    try {
      return client.hasSystemPermission(token, principal, ThriftHelper.convertEnum(perm, org.apache.accumulo.proxy.thrift.SystemPermission.class));
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public boolean hasTablePermission(String principal, String table, TablePermission perm) throws AccumuloException, AccumuloSecurityException {
    try {
      return client.hasTablePermission(token, principal, table, ThriftHelper.convertEnum(perm, org.apache.accumulo.proxy.thrift.TablePermission.class));
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public boolean hasNamespacePermission(String principal, String namespace, NamespacePermission perm) throws AccumuloException, AccumuloSecurityException {
    throw ExceptionFactory.unsupported();
  }

  public void grantSystemPermission(String principal, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    try {
      client.grantSystemPermission(token, principal, ThriftHelper.convertEnum(permission, org.apache.accumulo.proxy.thrift.SystemPermission.class));

    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void grantTablePermission(String principal, String table, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    try {
      client.grantTablePermission(token, principal, table, ThriftHelper.convertEnum(permission, org.apache.accumulo.proxy.thrift.TablePermission.class));

    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void grantNamespacePermission(String principal, String namespace, NamespacePermission permission) throws AccumuloException, AccumuloSecurityException {
    throw ExceptionFactory.unsupported();
  }

  public void revokeSystemPermission(String principal, SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    try {
      client.revokeSystemPermission(token, principal, ThriftHelper.convertEnum(permission, org.apache.accumulo.proxy.thrift.SystemPermission.class));

    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void revokeTablePermission(String principal, String table, TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    try {
      client.revokeTablePermission(token, principal, table, ThriftHelper.convertEnum(permission, org.apache.accumulo.proxy.thrift.TablePermission.class));

    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

  public void revokeNamespacePermission(String principal, String namespace, NamespacePermission permission) throws AccumuloException, AccumuloSecurityException {
    throw ExceptionFactory.unsupported();
  }

  @Deprecated
  public Set<String> listUsers() throws AccumuloException, AccumuloSecurityException {
    return listLocalUsers();
  }

  public Set<String> listLocalUsers() throws AccumuloException, AccumuloSecurityException {
    try {
      return client.listLocalUsers(token);
    } catch (TException e) {
      throw ExceptionFactory.accumuloException(e);
    }
  }

}
