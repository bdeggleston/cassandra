/*
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
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.service;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.Test;
import static org.junit.Assert.assertFalse;

import java.io.File;

public class StorageServiceClientTest
{
    public static final DatabaseDescriptor databaseDescriptor = SchemaLoader.databaseDescriptor;

    @Test
    public void testClientOnlyMode() throws ConfigurationException
    {
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        databaseDescriptor.getStorageService().initClient(0);

        // verify that no storage directories were created.
        for (String path : databaseDescriptor.getAllDataFileLocations())
        {
            assertFalse(new File(path).exists());
        }
        databaseDescriptor.getStorageService().stopClient();
    }
}
