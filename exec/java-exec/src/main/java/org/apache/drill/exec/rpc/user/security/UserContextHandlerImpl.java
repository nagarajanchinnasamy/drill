/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.user.security;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.server.options.OptionValue;
/**
 * Interface to handle user session context values of an external system.
 */
public class UserContextHandlerImpl implements UserContextHandler {

  /**
   * Setup to load context values from external system.
   */
  @Override
  public void setup(final DrillConfig drillConfig) throws DrillbitStartupException {
    
  };

  /**
   * Load context values from external system.
   */
  @Override
  public void load(final List<OptionValue> authenticatorOptions, final String sessionId) throws UserContextHandlingException {
    return;
  };

  /**
   * Close the handler. Used to release resources.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
	  
  }
}
