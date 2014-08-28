/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.auth;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Auth
{
    private static final Logger logger = LoggerFactory.getLogger(Auth.class);

    public static final Auth instance = new Auth();

    public static final String DEFAULT_SUPERUSER_NAME = "cassandra";

    private final long superuserSetupDelay;

    public static final String AUTH_KS = "system_auth";
    public static final String USERS_CF = "users";

    private static final String USERS_CF_SCHEMA = String.format("CREATE TABLE %s.%s ("
                                                                + "name text,"
                                                                + "super boolean,"
                                                                + "PRIMARY KEY(name)"
                                                                + ") WITH gc_grace_seconds=%d",
                                                                AUTH_KS,
                                                                USERS_CF,
                                                                90 * 24 * 60 * 60); // 3 months.

    private SelectStatement selectUserStatement;

    public Auth()
    {
        superuserSetupDelay = Long.getLong("cassandra.superuser_setup_delay_ms", 10000);
    }

    public long getSuperuserSetupDelay()
    {
        return superuserSetupDelay;
    }

    /**
     * Checks if the username is stored in AUTH_KS.USERS_CF.
     *
     * @param username Username to query.
     * @return whether or not Cassandra knows about the user.
     */
    public boolean isExistingUser(String username)
    {
        return !selectUser(username).isEmpty();
    }

    /**
     * Checks if the user is a known superuser.
     *
     * @param username Username to query.
     * @return true is the user is a superuser, false if they aren't or don't exist at all.
     */
    public boolean isSuperuser(String username)
    {
        UntypedResultSet result = selectUser(username);
        return !result.isEmpty() && result.one().getBoolean("super");
    }

    /**
     * Inserts the user into AUTH_KS.USERS_CF (or overwrites their superuser status as a result of an ALTER USER query).
     *
     * @param username Username to insert.
     * @param isSuper User's new status.
     * @throws RequestExecutionException
     */
    public void insertUser(String username, boolean isSuper) throws RequestExecutionException
    {
        QueryProcessor.process(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', %s)",
                                             AUTH_KS,
                                             USERS_CF,
                                             escape(username),
                                             isSuper),
                               consistencyForUser(username));
    }

    /**
     * Deletes the user from AUTH_KS.USERS_CF.
     *
     * @param username Username to delete.
     * @throws RequestExecutionException
     */
    public void deleteUser(String username) throws RequestExecutionException
    {
        QueryProcessor.process(String.format("DELETE FROM %s.%s WHERE name = '%s'",
                                             AUTH_KS,
                                             USERS_CF,
                                             escape(username)),
                               consistencyForUser(username));
    }

    /**
     * Sets up Authenticator and Authorizer.
     */
    public void setup()
    {
        if (DatabaseDescriptor.instance.getAuthenticator() instanceof AllowAllAuthenticator)
            return;

        setupAuthKeyspace();
        setupTable(USERS_CF, USERS_CF_SCHEMA);

        DatabaseDescriptor.instance.getAuthenticator().setup();
        DatabaseDescriptor.instance.getAuthorizer().setup();

        // register a custom MigrationListener for permissions cleanup after dropped keyspaces/cfs.
        MigrationManager.instance.register(new MigrationListener());

        // the delay is here to give the node some time to see its peers - to reduce
        // "Skipped default superuser setup: some nodes were not ready" log spam.
        // It's the only reason for the delay.
        StorageServiceExecutors.instance.tasks.schedule(new Runnable()
                                      {
                                          public void run()
                                          {
                                              setupDefaultSuperuser();
                                          }
                                      },
                                                        superuserSetupDelay,
                                      TimeUnit.MILLISECONDS);

        try
        {
            String query = String.format("SELECT * FROM %s.%s WHERE name = ?", AUTH_KS, USERS_CF);
            selectUserStatement = (SelectStatement) QueryProcessor.parseStatement(query).prepare().statement;
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
    }

    // Only use QUORUM cl for the default superuser.
    private ConsistencyLevel consistencyForUser(String username)
    {
        if (username.equals(DEFAULT_SUPERUSER_NAME))
            return ConsistencyLevel.QUORUM;
        else
            return ConsistencyLevel.LOCAL_ONE;
    }

    private void setupAuthKeyspace()
    {
        if (Schema.instance.getKSMetaData(AUTH_KS) == null)
        {
            try
            {
                KSMetaData ksm = KSMetaData.newKeyspace(AUTH_KS, SimpleStrategy.class.getName(), ImmutableMap.of("replication_factor", "1"), true);
                MigrationManager.instance.announceNewKeyspace(ksm, 0, false);
            }
            catch (Exception e)
            {
                throw new AssertionError(e); // shouldn't ever happen.
            }
        }
    }

    /**
     * Set up table from given CREATE TABLE statement under system_auth keyspace, if not already done so.
     *
     * @param name name of the table
     * @param cql CREATE TABLE statement
     */
    public void setupTable(String name, String cql)
    {
        if (Schema.instance.getCFMetaData(AUTH_KS, name) == null)
        {
            try
            {
                CFStatement parsed = (CFStatement)QueryProcessor.parseStatement(cql);
                parsed.prepareKeyspace(AUTH_KS);
                CreateTableStatement statement = (CreateTableStatement) parsed.prepare().statement;
                CFMetaData cfm = statement.getCFMetaData().copy(CFMetaData.generateLegacyCfId(AUTH_KS, name));
                assert cfm.cfName.equals(name);
                MigrationManager.instance.announceNewColumnFamily(cfm);
            }
            catch (Exception e)
            {
                throw new AssertionError(e);
            }
        }
    }

    private void setupDefaultSuperuser()
    {
        try
        {
            // insert a default superuser if AUTH_KS.USERS_CF is empty.
            if (!hasExistingUsers())
            {
                QueryProcessor.process(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', %s) USING TIMESTAMP 0",
                                                     AUTH_KS,
                                                     USERS_CF,
                                                     DEFAULT_SUPERUSER_NAME,
                                                     true),
                                       ConsistencyLevel.ONE);
                logger.info("Created default superuser '{}'", DEFAULT_SUPERUSER_NAME);
            }
        }
        catch (RequestExecutionException e)
        {
            logger.warn("Skipped default superuser setup: some nodes were not ready");
        }
    }

    private boolean hasExistingUsers() throws RequestExecutionException
    {
        // Try looking up the 'cassandra' default super user first, to avoid the range query if possible.
        String defaultSUQuery = String.format("SELECT * FROM %s.%s WHERE name = '%s'", AUTH_KS, USERS_CF, DEFAULT_SUPERUSER_NAME);
        String allUsersQuery = String.format("SELECT * FROM %s.%s LIMIT 1", AUTH_KS, USERS_CF);
        return !QueryProcessor.process(defaultSUQuery, ConsistencyLevel.ONE).isEmpty()
            || !QueryProcessor.process(defaultSUQuery, ConsistencyLevel.QUORUM).isEmpty()
            || !QueryProcessor.process(allUsersQuery, ConsistencyLevel.QUORUM).isEmpty();
    }

    // we only worry about one character ('). Make sure it's properly escaped.
    private String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    private UntypedResultSet selectUser(String username)
    {
        try
        {
            ResultMessage.Rows rows = selectUserStatement.execute(QueryState.forInternalCalls(),
                                                                  QueryOptions.forInternalCalls(consistencyForUser(username),
                                                                                                Lists.newArrayList(ByteBufferUtil.bytes(username))));
            return UntypedResultSet.create(rows.result);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * IMigrationListener implementation that cleans up permissions on dropped resources.
     */
    public class MigrationListener implements IMigrationListener
    {
        public void onDropKeyspace(String ksName)
        {
            DatabaseDescriptor.instance.getAuthorizer().revokeAll(DataResource.keyspace(ksName));
        }

        public void onDropColumnFamily(String ksName, String cfName)
        {
            DatabaseDescriptor.instance.getAuthorizer().revokeAll(DataResource.columnFamily(ksName, cfName));
        }

        public void onDropUserType(String ksName, String userType)
        {
        }

        public void onDropFunction(String namespace, String functionName)
        {
        }

        public void onCreateKeyspace(String ksName)
        {
        }

        public void onCreateColumnFamily(String ksName, String cfName)
        {
        }

        public void onCreateUserType(String ksName, String userType)
        {
        }

        public void onCreateFunction(String namespace, String functionName)
        {
        }

        public void onUpdateKeyspace(String ksName)
        {
        }

        public void onUpdateColumnFamily(String ksName, String cfName)
        {
        }

        public void onUpdateUserType(String ksName, String userType)
        {
        }

        public void onUpdateFunction(String namespace, String functionName)
        {
        }
    }
}
