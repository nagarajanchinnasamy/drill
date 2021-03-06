/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.user;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerUtil;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.proto.UserProtos.Property;
import org.apache.drill.exec.proto.UserProtos.UserProperties;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.SessionOptionManager;

import com.google.common.collect.Maps;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.StorageStrategy;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UserSession implements Closeable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserSession.class);

  public static final String SCHEMA = "schema";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String IMPERSONATION_TARGET = "impersonation_target";

  // known property names in lower case
  private static final Set<String> knownProperties = ImmutableSet.of(SCHEMA, USER, PASSWORD, IMPERSONATION_TARGET);

  private boolean supportComplexTypes = false;
  private UserCredentials credentials;
  private Map<String, String> properties;
  private OptionManager sessionOptions;
  private final AtomicInteger queryCount;
  private final String sessionId;

  /** Stores list of temporary tables, key is original table name converted to lower case to achieve case-insensitivity,
   *  value is generated table name. **/
  private final ConcurrentMap<String, String> temporaryTables;
  /** Stores list of session temporary locations, key is path to location, value is file system associated with location. **/
  private final ConcurrentMap<Path, FileSystem> temporaryLocations;

  /** On session close deletes all session temporary locations recursively and clears temporary locations list. */
  @Override
  public void close() {
    for (Map.Entry<Path, FileSystem> entry : temporaryLocations.entrySet()) {
      Path path = entry.getKey();
      FileSystem fs = entry.getValue();
      try {
        fs.delete(path, true);
        logger.info("Deleted session temporary location [{}] from file system [{}]",
            path.toUri().getPath(), fs.getUri());
      } catch (Exception e) {
        logger.warn("Error during session temporary location [{}] deletion from file system [{}]: [{}]",
            path.toUri().getPath(), fs.getUri(), e.getMessage());
      }
    }
    temporaryLocations.clear();
  }

  /**
   * Implementations of this interface are allowed to increment queryCount.
   * {@link org.apache.drill.exec.work.user.UserWorker} should have a member that implements the interface.
   * No other core class should implement this interface. Test classes may implement (see ControlsInjectionUtil).
   */
  public interface QueryCountIncrementer {
    void increment(final UserSession session);
  }

  public static class Builder {
    private UserSession userSession;

    public static Builder newBuilder() {
      return new Builder();
    }

    public Builder withCredentials(UserCredentials credentials) {
      userSession.credentials = credentials;
      return this;
    }

    public Builder withOptionManager(OptionManager systemOptions) {
      userSession.sessionOptions = new SessionOptionManager(systemOptions, userSession);
      return this;
    }

    public Builder withUserProperties(UserProperties properties) {
      userSession.properties = Maps.newHashMap();
      if (properties != null) {
        for (int i = 0; i < properties.getPropertiesCount(); i++) {
          final Property property = properties.getProperties(i);
          final String propertyName = property.getKey().toLowerCase();
          if (knownProperties.contains(propertyName)) {
            userSession.properties.put(propertyName, property.getValue());
          } else {
            logger.warn("Ignoring unknown property: {}", propertyName);
          }
        }
      }
      return this;
    }

    public Builder setSupportComplexTypes(boolean supportComplexTypes) {
      userSession.supportComplexTypes = supportComplexTypes;
      return this;
    }

    public UserSession build() {
      UserSession session = userSession;
      userSession = null;
      return session;
    }

    Builder() {
      userSession = new UserSession();
    }
  }

  private UserSession() {
    queryCount = new AtomicInteger(0);
    sessionId = UUID.randomUUID().toString();
    temporaryTables = Maps.newConcurrentMap();
    temporaryLocations = Maps.newConcurrentMap();
  }

  public boolean isSupportComplexTypes() {
    return supportComplexTypes;
  }

  public OptionManager getOptions() {
    return sessionOptions;
  }

  public UserCredentials getCredentials() {
    return credentials;
  }

  /**
   * Replace current user credentials with the given user's credentials. Meant to be called only by a
   * {@link InboundImpersonationManager impersonation manager}.
   *
   * @param impersonationManager impersonation manager making this call
   * @param newCredentials user credentials to change to
   */
  public void replaceUserCredentials(final InboundImpersonationManager impersonationManager,
                                     final UserCredentials newCredentials) {
    Preconditions.checkNotNull(impersonationManager, "User credentials can only be replaced by an" +
        " impersonation manager.");
    credentials = newCredentials;
  }

  public String getTargetUserName() {
    return properties.get(IMPERSONATION_TARGET);
  }

  public String getDefaultSchemaName() {
    return getProp(SCHEMA);
  }

  public void incrementQueryCount(final QueryCountIncrementer incrementer) {
    assert incrementer != null;
    queryCount.incrementAndGet();
  }

  public int getQueryCount() {
    return queryCount.get();
  }

  /**
   * Update the schema path for the session.
   * @param newDefaultSchemaPath New default schema path to set. It could be relative to the current default schema or
   *                             absolute schema.
   * @param currentDefaultSchema Current default schema.
   * @throws ValidationException If the given default schema path is invalid in current schema tree.
   */
  public void setDefaultSchemaPath(String newDefaultSchemaPath, SchemaPlus currentDefaultSchema)
      throws ValidationException {
    final List<String> newDefaultPathAsList = Lists.newArrayList(newDefaultSchemaPath.split("\\."));
    SchemaPlus newDefault;

    // First try to find the given schema relative to the current default schema.
    newDefault = SchemaUtilites.findSchema(currentDefaultSchema, newDefaultPathAsList);

    if (newDefault == null) {
      // If we fail to find the schema relative to current default schema, consider the given new default schema path as
      // absolute schema path.
      newDefault = SchemaUtilites.findSchema(currentDefaultSchema, newDefaultPathAsList);
    }

    if (newDefault == null) {
      SchemaUtilites.throwSchemaNotFoundException(currentDefaultSchema, newDefaultSchemaPath);
    }

    setProp(SCHEMA, SchemaUtilites.getSchemaPath(newDefault));
  }

  /**
   * @return Get current default schema path.
   */
  public String getDefaultSchemaPath() {
    return getProp(SCHEMA);
  }

  /**
   * Get default schema from current default schema path and given schema tree.
   * @param rootSchema root schema
   * @return A {@link org.apache.calcite.schema.SchemaPlus} object.
   */
  public SchemaPlus getDefaultSchema(SchemaPlus rootSchema) {
    final String defaultSchemaPath = getProp(SCHEMA);

    if (Strings.isNullOrEmpty(defaultSchemaPath)) {
      return null;
    }

    return SchemaUtilites.findSchema(rootSchema, defaultSchemaPath);
  }

  public boolean setSessionOption(String name, String value) {
    return true;
  }

  /**
   * @return unique session identifier
   */
  public String getSessionId() { return sessionId; }

  /**
   * Creates and adds session temporary location if absent using schema configuration.
   * Generates temporary table name and stores it's original name as key
   * and generated name as value in  session temporary tables cache.
   * Original temporary name is converted to lower case to achieve case-insensitivity.
   * If original table name already exists, new name is not regenerated and is reused.
   * This can happen if default temporary workspace was changed (file system or location) or
   * orphan temporary table name has remained (name was registered but table creation did not succeed).
   *
   * @param schema table schema
   * @param tableName original table name
   * @return generated temporary table name
   * @throws IOException if error during session temporary location creation
   */
  public String registerTemporaryTable(AbstractSchema schema, String tableName) throws IOException {
      addTemporaryLocation((WorkspaceSchemaFactory.WorkspaceSchema) schema);
      String temporaryTableName = Paths.get(sessionId, UUID.randomUUID().toString()).toString();
      String oldTemporaryTableName = temporaryTables.putIfAbsent(tableName.toLowerCase(), temporaryTableName);
      return oldTemporaryTableName == null ? temporaryTableName : oldTemporaryTableName;
  }

  /**
   * Returns generated temporary table name from the list of session temporary tables, null otherwise.
   * Original temporary name is converted to lower case to achieve case-insensitivity.
   *
   * @param tableName original table name
   * @return generated temporary table name
   */
  public String resolveTemporaryTableName(String tableName) {
    return temporaryTables.get(tableName.toLowerCase());
  }

  /**
   * Checks if passed table is temporary, table name is case-insensitive.
   * Before looking for table checks if passed schema is temporary and returns false if not
   * since temporary tables are allowed to be created in temporary workspace only.
   * If passed workspace is temporary, looks for temporary table.
   * First checks if table name is among temporary tables, if not returns false.
   * If temporary table named was resolved, checks that temporary table exists on disk,
   * to ensure that temporary table actually exists and resolved table name is not orphan
   * (for example, in result of unsuccessful temporary table creation).
   *
   * @param drillSchema table schema
   * @param config drill config
   * @param tableName original table name
   * @return true if temporary table exists in schema, false otherwise
   */
  public boolean isTemporaryTable(AbstractSchema drillSchema, DrillConfig config, String tableName) {
    if (!SchemaUtilites.isTemporaryWorkspace(drillSchema.getFullSchemaName(), config)) {
      return false;
    }
    String temporaryTableName = resolveTemporaryTableName(tableName);
    if (temporaryTableName != null) {
      Table temporaryTable = SqlHandlerUtil.getTableFromSchema(drillSchema, temporaryTableName);
      if (temporaryTable != null && temporaryTable.getJdbcTableType() == Schema.TableType.TABLE) {
        return true;
      }
    }
    return false;
  }

  /**
   * Removes temporary table name from the list of session temporary tables.
   * Original temporary name is converted to lower case to achieve case-insensitivity.
   *
   * @param tableName original table name
   */
  public void removeTemporaryTable(AbstractSchema drillSchema, String tableName) {
    String temporaryTable = resolveTemporaryTableName(tableName);
    if (temporaryTable == null) {
      return;
    }
    SqlHandlerUtil.dropTableFromSchema(drillSchema, temporaryTable);
    temporaryTables.remove(tableName.toLowerCase());
  }

  /**
   * Session temporary tables are stored under temporary workspace location in session folder
   * defined by unique session id. These session temporary locations are deleted on session close.
   * If default temporary workspace file system or location is changed at runtime,
   * new session temporary location will be added with corresponding file system
   * to the list of session temporary locations. If location does not exist it will be created and
   * {@link StorageStrategy#TEMPORARY} storage rules will be applied to it.
   *
   * @param temporaryWorkspace temporary workspace
   * @throws IOException in case of error during temporary location creation
   */
  private void addTemporaryLocation(WorkspaceSchemaFactory.WorkspaceSchema temporaryWorkspace) throws IOException {
    DrillFileSystem fs = temporaryWorkspace.getFS();
    Path temporaryLocation = new Path(Paths.get(fs.getUri().toString(),
        temporaryWorkspace.getDefaultLocation(), sessionId).toString());

    FileSystem fileSystem = temporaryLocations.putIfAbsent(temporaryLocation, fs);

    if (fileSystem == null) {
      StorageStrategy.TEMPORARY.createPathAndApply(fs, temporaryLocation);
      Preconditions.checkArgument(fs.exists(temporaryLocation),
          String.format("Temporary location should exist [%s]", temporaryLocation.toUri().getPath()));
    }
  }

  private String getProp(String key) {
    return properties.get(key) != null ? properties.get(key) : "";
  }

  private void setProp(String key, String value) {
    properties.put(key, value);
  }
}
