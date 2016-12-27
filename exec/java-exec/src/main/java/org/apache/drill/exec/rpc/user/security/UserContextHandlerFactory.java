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

import static org.apache.drill.exec.ExecConstants.USER_CONTEXT_HANDLER_IMPL;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.SystemOptionManager;

import com.google.common.base.Strings;

/**
 * Factory class which provides {@link org.apache.drill.exec.rpc.user.security.UserContextHandler} implementation
 * based on the BOOT options.
 */
public class UserContextHandlerFactory {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserContextHandlerFactory.class);

  /**
   * Create a {@link org.apache.drill.exec.rpc.user.security.UserContextHandler} implementation.
   *
   */
  public static UserContextHandler createHandler(final DrillConfig config, ScanResult scan) throws DrillbitStartupException {
    final String handlerImplConfigured = config.getString(USER_CONTEXT_HANDLER_IMPL);

    if (Strings.isNullOrEmpty(handlerImplConfigured)) {
      throw new DrillbitStartupException(String.format("Invalid value '%s' for BOOT option '%s'", handlerImplConfigured,
        USER_CONTEXT_HANDLER_IMPL));
    }

    final Collection<Class<? extends UserContextHandler>> handlerImpls =
        scan.getImplementations(UserContextHandler.class);

    for(Class<? extends UserContextHandler> clazz : handlerImpls) {
      final UserContextHandlerTemplate template = clazz.getAnnotation(UserContextHandlerTemplate.class);
      if (template == null) {
        logger.warn("{} doesn't have {} annotation. Skipping.", clazz.getCanonicalName(), UserContextHandlerTemplate.class);
        continue;
      }

      if (Strings.isNullOrEmpty(template.type())) {
        logger.warn("{} annotation doesn't have valid type field for UserContextHandler implementation {}. Skipping..",
          UserContextHandlerTemplate.class, clazz.getCanonicalName());
        continue;
      }

      if (template.type().equalsIgnoreCase(handlerImplConfigured)) {
        Constructor<?> validConstructor = null;
        for (Constructor<?> c : clazz.getConstructors()) {
          if (c.getParameterTypes().length == 0) {
            validConstructor = c;
            break;
          }
        }

        if (validConstructor == null) {
          logger.warn("Skipping UserContextHandler implementation class '{}' since it doesn't " +
              "implement a constructor [{}()]", clazz.getCanonicalName(), clazz.getName());
          continue;
        }

        // Instantiate user context handler and initialize it
        try {
          final UserContextHandler contextHandler = clazz.newInstance();
          contextHandler.setup(config);
          return contextHandler;
        } catch(IllegalArgumentException | IllegalAccessException | InstantiationException e) {
          throw new DrillbitStartupException(
              String.format("Failed to create and initialize the UserContextHandler class '%s'",
                  clazz.getCanonicalName()), e);
        }
      }
    }

    String errMsg = String.format("Failed to find the implementation of '%s' for type '%s'",
        UserContextHandler.class.getCanonicalName(), handlerImplConfigured);
    logger.error(errMsg);
    throw new DrillbitStartupException(errMsg);
  }
}
