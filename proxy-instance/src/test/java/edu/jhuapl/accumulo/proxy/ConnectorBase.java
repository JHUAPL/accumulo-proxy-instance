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
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.proxy.Proxy;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;

/**
 * Base class for unit and integration tests that want to get access to an Instance and/or Connector. This class provides the means to setup a ProxyInstace
 * connection to a local ProxyServer backed by a mock (in-memory) Accumulo instance for unit tests, or to create a ProxyInstace connection to a real (external)
 * ProxyServer for integration tests.
 */
public class ConnectorBase {

  protected static volatile ProxyInstance instance = null;
  protected static volatile Connector connector = null;
  private static Thread proxyServerThread = null;

  /**
   * Initializes the instance and connector variables (and proxyServerThread, if needed) if they are currently null. This will be done once per class, but we
   * cannot do this with a @BeforeClass because we want to use the type of the subclass to determine if it is a unit or integration test.
   */
  @Before
  public void prepare() throws TTransportException, AccumuloException, AccumuloSecurityException, InterruptedException {
    if (instance == null) {
      synchronized (ConnectorBase.class) {
        if (instance == null) {
          ProxyInstance locInst = null;
          Connector locConn = null;
          if (IntegrationTest.class.isAssignableFrom(getClass())) {
            String host = getString("accumulo.proxy.host");
            int port = getInt("accumulo.proxy.port");
            String user = getString("accumulo.proxy.user");
            String passwd = getString("accumulo.proxy.password");
            locInst = new ProxyInstance(host, port);
            locConn = locInst.getConnector(user, new PasswordToken(passwd));
          } else {
            createLocalServer();
            locInst = new ProxyInstance("localhost", 45678);
            locConn = locInst.getConnector("root", new PasswordToken(""));
          }
          instance = locInst;
          connector = locConn;
        }
      }
    }
  }

  @AfterClass
  public static void teardown() {
    try {
      if (instance != null) {
        instance.close();
      }
    } finally {
      instance = null;
    }

    try {
      if (proxyServerThread != null) {
        proxyServerThread.interrupt();
      }
    } finally {
      proxyServerThread = null;
    }
  }

  private static String getString(String key) {
    String val = System.getProperty(key);
    if (val == null) {
      Assert.fail("You must specify the system property: " + key);
    }
    return val.trim();
  }

  private static int getInt(String key) throws NumberFormatException {
    return Integer.parseInt(getString(key));
  }

  private static void createLocalServer() throws InterruptedException {
    proxyServerThread = new Thread() {
      public void run() {
        try {
          String[] args = new String[] {"-p", "src/test/resources/mock.props"};
          Proxy.main(args);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    };
    proxyServerThread.setDaemon(true);
    proxyServerThread.start();
    Thread.sleep(150);
  }

}
