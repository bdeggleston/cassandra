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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.auth.AuthKeyspace.RESOURCE_ROLE_INDEX;
import static org.apache.cassandra.auth.AuthKeyspace.ROLES;
import static org.apache.cassandra.auth.AuthKeyspace.ROLE_MEMBERS;
import static org.apache.cassandra.auth.AuthKeyspace.ROLE_PERMISSIONS;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;

public class CassandraAuthorizerTest
{
    private static AuthenticatedUser USER;
    private static RoleResource GRANTEE;
    private static LocalCassandraAuthorizer AUTHORIZER;
    private static DataResource TABLE;
    private static Set<String> NO_DCS = ImmutableSet.of();

    private static class LocalCassandraAuthorizer extends CassandraAuthorizer
    {
        protected UntypedResultSet process(String query) throws RequestExecutionException
        {
            return QueryProcessor.executeInternal(query);
        }

        private String dc = "DC1";

        protected String getLocalDC()
        {
            return dc;
        }

        public void setLocalDc(String dc)
        {
            this.dc = dc;
        }

        protected UntypedResultSet authorizeRole(IResource resource, RoleResource role)
        {
            QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                                                                 Lists.newArrayList(ByteBufferUtil.bytes(role.getRoleName()),
                                                                                    ByteBufferUtil.bytes(resource.getName())));

            ResultMessage.Rows rows = authorizeRoleStatement.executeInternal(QueryState.forInternalCalls(), options);
            return UntypedResultSet.create(rows.result);
        }
    }

    private static class LocalCassandraRoleManager extends CassandraRoleManager
    {
        protected UntypedResultSet process(String query, ConsistencyLevel consistencyLevel) throws RequestValidationException, RequestExecutionException
        {
            return QueryProcessor.executeInternal(query);
        }

        public Set<RoleResource> getRoles(RoleResource grantee, boolean includeInherited) throws RequestValidationException, RequestExecutionException
        {
            return Sets.newHashSet(grantee);
        }

        public boolean isSuper(RoleResource role)
        {
            return false;
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.setupAuth(new LocalCassandraRoleManager(), new PasswordAuthenticator(), new LocalCassandraAuthorizer());

        USER = new AuthenticatedUser("someguy");
        GRANTEE = RoleResource.role("user");
        AUTHORIZER = (LocalCassandraAuthorizer) DatabaseDescriptor.getAuthorizer();
        TABLE = DataResource.table("ks", "tbl");
    }

    private static void truncateAuthTable(String name)
    {
        Keyspace.open(AUTH_KEYSPACE_NAME).getColumnFamilyStore(name).truncateBlocking();
    }

    @Before
    public void setUp() throws Exception
    {
        truncateAuthTable(ROLES);
        truncateAuthTable(ROLE_MEMBERS);
        truncateAuthTable(ROLE_PERMISSIONS);
        truncateAuthTable(RESOURCE_ROLE_INDEX);
    }

    private static UntypedResultSet query(String q)
    {
        return QueryProcessor.executeInternal(q);
    }

    private static Set<Permission> perms(Permission... p)
    {
        return Sets.newHashSet(p);
    }

    private static Set<String> dcs(String... d)
    {
        return Sets.newHashSet(d);
    }

    private static void assertTableSize(String table, int expectedSize)
    {
        Assert.assertEquals(expectedSize, query(String.format("SELECT * FROM %s.%s", AUTH_KEYSPACE_NAME, table)).size());
    }

    private static void assertPermissionExists(RoleResource role, DataResource resource, Permission permission, Set<String> expectedDcs)
    {
        UntypedResultSet rows = query(String.format("SELECT * FROM %s.%s", AUTH_KEYSPACE_NAME, ROLE_PERMISSIONS));
        for (UntypedResultSet.Row row: rows)
        {
            if (row.getString("role").equals(role.getRoleName()) && row.getString("resource").equals(resource.getName()))
            {
                Set<String> permissions = row.getSet("permissions", UTF8Type.instance);
                Assert.assertTrue(String.format("Permission %s not found in %s", permission, permissions), permissions.contains(permission.name()));
                Map<String, Set<String>> datacenters = row.has("datacenters")
                                                       ? row.getMap("datacenters", UTF8Type.instance, SetType.getInstance(UTF8Type.instance, false))
                                                       : Collections.emptyMap();
                Set<String> actualDcs = datacenters.getOrDefault(permission.name(), Collections.emptySet());
                Assert.assertEquals(expectedDcs, actualDcs);
                return;
            }
        }
        Assert.fail(String.format("didn't find permission %s %s %s %s in %s", role, resource, permission, expectedDcs, rows));
    }

    private static Set<Permission> getPermissions(RoleResource role, DataResource resource)
    {
        UntypedResultSet rows = query(String.format("SELECT * FROM %s.%s WHERE role='%s' AND resource='%s'",
                                                    AUTH_KEYSPACE_NAME, ROLE_PERMISSIONS, role.getRoleName(), resource.getName()));
        if (rows.isEmpty())
        {
            return Collections.emptySet();
        }
        Set<String> stringPerms = Iterables.getOnlyElement(rows).getSet("permissions", UTF8Type.instance);
        ImmutableSet.Builder<Permission> permissionBuilder = ImmutableSet.builder();
        if (stringPerms != null)
        {
            stringPerms.forEach(p -> permissionBuilder.add(Permission.valueOf(p)));
        }
        return permissionBuilder.build();
    }

    private static void assertPermissionDoesntExist(RoleResource role, DataResource resource, Permission permission)
    {
        for (UntypedResultSet.Row row: query(String.format("SELECT * FROM %s.%s", AUTH_KEYSPACE_NAME, ROLE_PERMISSIONS)))
        {
            if (row.getString("role").equals(role.getName()) && row.getString("resource").equals(resource.getName()))
            {
                Assert.assertFalse(String.format("Unexpectedly found permission %s %s %s", role, resource, permission),
                                   row.getSet("permissions", UTF8Type.instance).contains(permission.name()));
            }
        }
    }

    private static void assertRoleIndexEntry(RoleResource role, DataResource resource, boolean expectedExists)
    {
        boolean actualExists = !query(String.format("SELECT * FROM %s.%s WHERE resource='%s' AND role='%s'",
                                                    AUTH_KEYSPACE_NAME, RESOURCE_ROLE_INDEX, resource.getName(), role.getRoleName())).isEmpty();

        if (expectedExists)
        {
            Assert.assertTrue(String.format("Expected to find role index entry %s %s, but didn't", role, resource), actualExists);
        }
        else
        {
            Assert.assertFalse(String.format("Unexpectedly found role index entry %s %s", role, resource), actualExists);
        }
    }

    private static void assertRoleIndexEntryExists(RoleResource role, DataResource resource)
    {
        assertRoleIndexEntry(role, resource, true);
    }

    private static void assertRoleIndexEntryDoesntExist(RoleResource role, DataResource resource)
    {
        assertRoleIndexEntry(role, resource, false);
    }

    @Test
    public void grantPermission() throws Exception
    {
        assertTableSize(ROLE_PERMISSIONS, 0);
        assertTableSize(RESOURCE_ROLE_INDEX, 0);

        AUTHORIZER.grant(USER, perms(Permission.CREATE, Permission.ALTER), TABLE, GRANTEE, dcs());

        assertTableSize(ROLE_PERMISSIONS, 1);
        assertTableSize(RESOURCE_ROLE_INDEX, 1);

        assertPermissionExists(GRANTEE, TABLE, Permission.CREATE, NO_DCS);
        assertPermissionExists(GRANTEE, TABLE, Permission.ALTER, NO_DCS);
        assertRoleIndexEntryExists(GRANTEE, TABLE);
    }

    @Test
    public void revokePermission() throws Exception
    {
        AUTHORIZER.grant(USER, perms(Permission.CREATE, Permission.ALTER), TABLE, GRANTEE, dcs());
        assertPermissionExists(GRANTEE, TABLE, Permission.CREATE, NO_DCS);
        assertPermissionExists(GRANTEE, TABLE, Permission.ALTER, NO_DCS);
        assertRoleIndexEntryExists(GRANTEE, TABLE);

        AUTHORIZER.revoke(USER, perms(Permission.CREATE), TABLE, GRANTEE);
        assertPermissionDoesntExist(GRANTEE, TABLE, Permission.CREATE);
        assertPermissionExists(GRANTEE, TABLE, Permission.ALTER, NO_DCS);
//        assertRoleIndexEntryExists(GRANTEE, TABLE); // TODO: seems to be an existing bug here
    }

    /**
     * We should add entries to the datacenter map for permissions that support it
     */
    @Test
    public void grantDcPermission() throws Exception
    {
        Set<String> expectedDcs = dcs("DC1", "DC2");
        AUTHORIZER.grant(USER, perms(Permission.SELECT, Permission.MODIFY, Permission.EXECUTE), TABLE, GRANTEE, expectedDcs);
        assertTableSize(ROLE_PERMISSIONS, 1);
        Assert.assertEquals(perms(Permission.SELECT, Permission.MODIFY, Permission.EXECUTE), getPermissions(GRANTEE, TABLE));
        assertPermissionExists(GRANTEE, TABLE, Permission.SELECT, expectedDcs);
        assertPermissionExists(GRANTEE, TABLE, Permission.MODIFY, expectedDcs);
        assertPermissionExists(GRANTEE, TABLE, Permission.EXECUTE, expectedDcs);
    }

    /**
     * We shouldn't add entries to the datacenter map for permissions that don't support it
     */
    @Test
    public void grantNonDcPermission() throws Exception
    {
        Set<String> expectedDcs = dcs("DC1", "DC2");
        AUTHORIZER.grant(USER, perms(Permission.SELECT, Permission.ALTER), TABLE, GRANTEE, expectedDcs);
        assertTableSize(ROLE_PERMISSIONS, 1);
        Assert.assertEquals(perms(Permission.SELECT, Permission.ALTER), getPermissions(GRANTEE, TABLE));
        assertPermissionExists(GRANTEE, TABLE, Permission.SELECT, expectedDcs);
        assertPermissionExists(GRANTEE, TABLE, Permission.ALTER, NO_DCS);
    }

    /**
     * We should remove entries from the datacenter map for revoked permissions only
     */
    @Test
    public void revokeDcPermission() throws Exception
    {
        Set<String> expectedDcs = dcs("DC1", "DC2");
        AUTHORIZER.grant(USER, perms(Permission.SELECT, Permission.MODIFY, Permission.ALTER), TABLE, GRANTEE, expectedDcs);
        assertTableSize(ROLE_PERMISSIONS, 1);
        Assert.assertEquals(perms(Permission.SELECT, Permission.MODIFY, Permission.ALTER), getPermissions(GRANTEE, TABLE));
        assertPermissionExists(GRANTEE, TABLE, Permission.SELECT, expectedDcs);
        assertPermissionExists(GRANTEE, TABLE, Permission.MODIFY, expectedDcs);
        assertPermissionExists(GRANTEE, TABLE, Permission.ALTER, NO_DCS);

        AUTHORIZER.revoke(USER, perms(Permission.MODIFY), TABLE, GRANTEE);
        Assert.assertEquals(perms(Permission.SELECT, Permission.ALTER), getPermissions(GRANTEE, TABLE));
        assertPermissionExists(GRANTEE, TABLE, Permission.SELECT, expectedDcs);
        assertPermissionDoesntExist(GRANTEE, TABLE, Permission.MODIFY);
        assertPermissionExists(GRANTEE, TABLE, Permission.ALTER, NO_DCS);
    }

    /**
     * Revoking non-dc permission shouldn't affect the datacenter map
     */
    @Test
    public void revokeNonDcPermission() throws Exception
    {
        Set<String> expectedDcs = dcs("DC1", "DC2");
        AUTHORIZER.grant(USER, perms(Permission.SELECT, Permission.MODIFY, Permission.ALTER), TABLE, GRANTEE, expectedDcs);
        assertTableSize(ROLE_PERMISSIONS, 1);
        Assert.assertEquals(perms(Permission.SELECT, Permission.MODIFY, Permission.ALTER), getPermissions(GRANTEE, TABLE));
        assertPermissionExists(GRANTEE, TABLE, Permission.SELECT, expectedDcs);
        assertPermissionExists(GRANTEE, TABLE, Permission.MODIFY, expectedDcs);
        assertPermissionExists(GRANTEE, TABLE, Permission.ALTER, NO_DCS);

        AUTHORIZER.revoke(USER, perms(Permission.ALTER), TABLE, GRANTEE);
        Assert.assertEquals(perms(Permission.SELECT, Permission.MODIFY), getPermissions(GRANTEE, TABLE));
        assertPermissionExists(GRANTEE, TABLE, Permission.SELECT, expectedDcs);
        assertPermissionExists(GRANTEE, TABLE, Permission.MODIFY, expectedDcs);
        assertPermissionDoesntExist(GRANTEE, TABLE, Permission.ALTER);
    }

    /**
     * Permissions granted without any dcs specified should always return granted permission
     */
    @Test
    public void authorizeNoDcPermission() throws Exception
    {
        AUTHORIZER.setLocalDc("DC1");
        AUTHORIZER.grant(USER, perms(Permission.SELECT), TABLE, USER.getPrimaryRole(), NO_DCS);

        Assert.assertTrue(AUTHORIZER.authorize(USER, TABLE).contains(Permission.SELECT));
    }

    /**
     * Permissions granted with the local dc specified should return the granted permission
     */
    @Test
    public void authorizeDcPermission() throws Exception
    {
        AUTHORIZER.setLocalDc("DC1");
        AUTHORIZER.grant(USER, perms(Permission.SELECT), TABLE, USER.getPrimaryRole(), dcs("DC1"));

        Assert.assertTrue(AUTHORIZER.authorize(USER, TABLE).contains(Permission.SELECT));
    }

    /**
     * Permissions granted with dcs other than the local one should not be returned
     */
    @Test
    public void authorizeOtherDcPermission() throws Exception
    {
        AUTHORIZER.setLocalDc("DC2");
        AUTHORIZER.grant(USER, perms(Permission.SELECT), TABLE, USER.getPrimaryRole(), dcs("DC1"));

        Assert.assertFalse(AUTHORIZER.authorize(USER, TABLE).contains(Permission.SELECT));
    }

    /**
     * Permissions that don't support dc granularity should never be filtered
     */
    @Test
    public void authorizeNonDcPermission() throws Exception
    {
        AUTHORIZER.setLocalDc("DC2");
        AUTHORIZER.grant(USER, perms(Permission.ALTER), TABLE, USER.getPrimaryRole(), dcs());

        Assert.assertTrue(AUTHORIZER.authorize(USER, TABLE).contains(Permission.ALTER));
    }
}
