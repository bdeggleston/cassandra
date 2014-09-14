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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static final Auth instance;
    static
    {
        Auth auth = null;

        try
        {
            auth = new Auth(DatabaseDescriptor.instance.getConfig(), DatabaseDescriptor.instance);
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal configuration error", e);
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start. See log for stacktrace.");
            System.exit(1);
        }
        catch (Exception e)
        {
            logger.error("Fatal error during configuration loading", e);
            System.err.println(e.getMessage() + "\nFatal error during configuration loading; unable to start. See log for stacktrace.");
            System.exit(1);
        }

        instance = auth;
    }

    public static final String DEFAULT_SUPERUSER_NAME = "cassandra";

    private static final long SUPERUSER_SETUP_DELAY = Long.getLong("cassandra.superuser_setup_delay_ms", 10000);

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
    private final Config conf;
    private final IAuthorizer authorizer;
    private final IAuthenticator authenticator;
    private final IInternodeAuthenticator internodeAuthenticator;

    private final Set<IResource> readableSystemResources = new HashSet<>();
    private final Set<IResource> protectedAuthResources = new HashSet<>();

    // User-level permissions cache.
    private final LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> permissionsCache;

    private final DatabaseDescriptor databaseDescriptor;

    public Auth(Config conf, DatabaseDescriptor databaseDescriptor) throws ConfigurationException
    {
        this.conf = conf;
        this.databaseDescriptor = databaseDescriptor;
        authorizer = createAuthorizer();
        authenticator = createAuthenticator();
        internodeAuthenticator = createInternodeAuthenticator();

        if (authenticator instanceof AllowAllAuthenticator && !(authorizer instanceof AllowAllAuthorizer))
            throw new ConfigurationException("AllowAllAuthenticator can't be used with " +  this.conf.authorizer);

        authenticator.validateConfiguration();
        authorizer.validateConfiguration();
        internodeAuthenticator.validateConfiguration();

        // We want these system cfs to be always readable since many tools rely on them (nodetool, cqlsh, bulkloader, etc.)
        String[] cfs =  new String[] { SystemKeyspace.LOCAL_CF,
                SystemKeyspace.PEERS_CF,
                SystemKeyspace.SCHEMA_KEYSPACES_CF,
                SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF,
                SystemKeyspace.SCHEMA_COLUMNS_CF,
                SystemKeyspace.SCHEMA_USER_TYPES_CF};
        for (String cf : cfs)
            readableSystemResources.add(DataResource.columnFamily(Keyspace.SYSTEM_KS, cf, getSchema()));

        protectedAuthResources.addAll(authenticator.protectedResources());
        protectedAuthResources.addAll(authorizer.protectedResources());

        permissionsCache = initPermissionsCache();
    }

    private IAuthorizer createAuthorizer() throws ConfigurationException
    {
        if (conf.authorizer == null)
            return new AllowAllAuthorizer();

        String className = conf.authorizer;
        if (!className.contains("."))
            className = "org.apache.cassandra.auth." + className;
        Class<IAuthorizer> cls = FBUtilities.classForName(className, "authorizer");
        Constructor<IAuthorizer> constructor = FBUtilities.getConstructor(cls, Auth.class);
        try
        {
            return constructor != null ? constructor.newInstance(this) : cls.newInstance();
        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException e)
        {
            String msg = String.format(
                    "Error instantiating IAuthorizer: %s" +
                            "IAuthorizer implementations must support empty constructors, \n" +
                            "or constructors taking a single org.apache.cassandra.auth.Auth argument",
                    className);
            throw new ConfigurationException(msg, e);
        }
    }

    private IAuthenticator createAuthenticator() throws ConfigurationException
    {
        if (conf.authenticator == null)
            return new AllowAllAuthenticator();

        String className = conf.authenticator;
        if (!className.contains("."))
            className = "org.apache.cassandra.auth." + className;
        Class<IAuthenticator> cls = FBUtilities.classForName(className, "authorizer");

        // look for a constructor that takes an auth instance

        Constructor<IAuthenticator> constructor = FBUtilities.getConstructor(cls, Auth.class);
        try
        {
            return constructor != null ? constructor.newInstance(this) : cls.newInstance();
        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException e)
        {
            String msg = String.format(
                    "Error instantiating IAuthenticator: %s.\n" +
                            "IAuthorizer implementations must support empty constructors, \n" +
                            "or constructors taking a single org.apache.cassandra.auth.Auth argument",
                    className);
            throw new ConfigurationException(msg, e);
        }
    }

    private IInternodeAuthenticator createInternodeAuthenticator() throws ConfigurationException
    {
        if (conf.internode_authenticator == null)
            return new AllowAllInternodeAuthenticator();

        String className = conf.internode_authenticator;
        if (!className.contains("."))
            className = "org.apache.cassandra.auth." + className;
        Class<IInternodeAuthenticator> cls = FBUtilities.classForName(className, "authorizer");
        Constructor<IInternodeAuthenticator> constructor = FBUtilities.getConstructor(cls, Auth.class);
        try
        {
            return constructor != null ? constructor.newInstance(this) : cls.newInstance();
        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException e)
        {
            String msg = String.format(
                    "Error instantiating IInternodeAuthenticator: %s" +
                            "IAuthorizer implementations must support empty constructors, \n" +
                            "or constructors taking a single org.apache.cassandra.auth.Auth argument",
                    className);
            throw new ConfigurationException(msg, e);
        }
    }

    public static long getSuperuserSetupDelay()
    {
        return SUPERUSER_SETUP_DELAY;
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
        databaseDescriptor.getQueryProcessor().process(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', %s)",
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
        databaseDescriptor.getQueryProcessor().process(String.format("DELETE FROM %s.%s WHERE name = '%s'",
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
        if (authenticator instanceof AllowAllAuthenticator)
            return;

        setupAuthKeyspace();
        setupTable(USERS_CF, USERS_CF_SCHEMA);

        authenticator.setup();
        authorizer.setup();

        // register a custom MigrationListener for permissions cleanup after dropped keyspaces/cfs.
        databaseDescriptor.getMigrationManager().register(new MigrationListener());

        // the delay is here to give the node some time to see its peers - to reduce
        // "Skipped default superuser setup: some nodes were not ready" log spam.
        // It's the only reason for the delay.
        databaseDescriptor.getStorageServiceExecutors().tasks.schedule(new Runnable()
                                      {
                                          public void run()
                                          {
                                              setupDefaultSuperuser();
                                          }
                                      }, SUPERUSER_SETUP_DELAY, TimeUnit.MILLISECONDS);

        try
        {
            String query = String.format("SELECT * FROM %s.%s WHERE name = ?", AUTH_KS, USERS_CF);
            selectUserStatement = (SelectStatement) databaseDescriptor.getQueryProcessor().parseStatement(query).prepare().statement;
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
        if (databaseDescriptor.getSchema().getKSMetaData(AUTH_KS) == null)
        {
            try
            {
                KSMetaData ksm = databaseDescriptor.getKSMetaDataFactory().newKeyspace(AUTH_KS, SimpleStrategy.class.getName(), ImmutableMap.of("replication_factor", "1"), true);
                databaseDescriptor.getMigrationManager().announceNewKeyspace(ksm, 0, false);
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
        if (databaseDescriptor.getSchema().getCFMetaData(AUTH_KS, name) == null)
        {
            try
            {
                CFStatement parsed = (CFStatement)databaseDescriptor.getQueryProcessor().parseStatement(cql);
                parsed.prepareKeyspace(AUTH_KS);
                CreateTableStatement statement = (CreateTableStatement) parsed.prepare().statement;
                CFMetaData cfm = statement.getCFMetaData().copy(CFMetaData.generateLegacyCfId(AUTH_KS, name));
                assert cfm.cfName.equals(name);
                databaseDescriptor.getMigrationManager().announceNewColumnFamily(cfm);
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
                databaseDescriptor.getQueryProcessor().process(String.format("INSERT INTO %s.%s (name, super) VALUES ('%s', %s) USING TIMESTAMP 0",
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
        return !databaseDescriptor.getQueryProcessor().process(defaultSUQuery, ConsistencyLevel.ONE).isEmpty()
            || !databaseDescriptor.getQueryProcessor().process(defaultSUQuery, ConsistencyLevel.QUORUM).isEmpty()
            || !databaseDescriptor.getQueryProcessor().process(allUsersQuery, ConsistencyLevel.QUORUM).isEmpty();
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
            ResultMessage.Rows rows = selectUserStatement.execute(QueryState.forInternalCalls(databaseDescriptor.getTracing(), this),
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

    public IAuthenticator getAuthenticator()
    {
        return authenticator;
    }

    public IAuthorizer getAuthorizer()
    {
        return authorizer;
    }

    public IInternodeAuthenticator getInternodeAuthenticator()
    {
        return internodeAuthenticator;
    }

    private LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> initPermissionsCache()
    {
        if (authorizer instanceof AllowAllAuthorizer)
            return null;

        int validityPeriod = databaseDescriptor.getPermissionsValidity();
        if (validityPeriod <= 0)
            return null;

        return CacheBuilder.newBuilder().expireAfterWrite(validityPeriod, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<Pair<AuthenticatedUser, IResource>, Set<Permission>>()
                {
                    public Set<Permission> load(Pair<AuthenticatedUser, IResource> userResource)
                    {
                        return authorizer.authorize(userResource.left,
                                                                                     userResource.right);
                    }
                });
    }

    public LoadingCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> getPermissionsCache()
    {
        return permissionsCache;
    }

    public Set<IResource> getProtectedAuthResources()
    {
        return protectedAuthResources;
    }

    public Set<IResource> getReadableSystemResources()
    {
        return readableSystemResources;
    }

    public QueryProcessor getQueryProcessor()
    {
        return databaseDescriptor.getQueryProcessor();
    }

    public Tracing getTracing()
    {
        return databaseDescriptor.getTracing();
    }

    public Schema getSchema()
    {
        return databaseDescriptor.getSchema();
    }

    public StorageServiceExecutors getStorageServiceExecutors()
    {
        return databaseDescriptor.getStorageServiceExecutors();
    }

    /**
     * IMigrationListener implementation that cleans up permissions on dropped resources.
     */
    public class MigrationListener implements IMigrationListener
    {
        public void onDropKeyspace(String ksName)
        {
            authorizer.revokeAll(DataResource.keyspace(ksName, getSchema()));
        }

        public void onDropColumnFamily(String ksName, String cfName)
        {
            authorizer.revokeAll(DataResource.columnFamily(ksName, cfName, getSchema()));
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
