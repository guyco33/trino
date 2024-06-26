/*
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
package io.trino.plugin.accumulo;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.io.Text;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_DNE;
import static io.trino.plugin.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_EXISTS;
import static io.trino.plugin.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static java.util.Objects.requireNonNull;

/**
 * This class is a light wrapper for Accumulo's Connector object.
 * It will perform the given operation, or throw an exception if an Accumulo- or ZooKeeper-based error occurs.
 */
public class AccumuloTableManager
{
    private static final Logger LOG = Logger.get(AccumuloTableManager.class);
    private final AccumuloClient client;

    @Inject
    public AccumuloTableManager(AccumuloClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    public void createNamespace(String schema)
    {
        try {
            client.namespaceOperations().create(schema);
        }
        catch (NamespaceExistsException e) {
            throw new TrinoException(ALREADY_EXISTS, "Namespace already exists: " + schema, e);
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new TrinoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to create Accumulo namespace: " + schema, e);
        }
    }

    public void dropNamespace(String schema)
    {
        try {
            client.namespaceOperations().delete(schema);
        }
        catch (AccumuloException | AccumuloSecurityException | NamespaceNotFoundException | NamespaceNotEmptyException e) {
            throw new TrinoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to delete Accumulo namespace: " + schema, e);
        }
    }

    public boolean namespaceExists(String schema)
    {
        try {
            return client.namespaceOperations().exists(schema);
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new TrinoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to check for existence Accumulo namespace: " + schema, e);
        }
    }

    public boolean exists(String table)
    {
        return client.tableOperations().exists(table);
    }

    public void createAccumuloTable(String table)
    {
        try {
            client.tableOperations().create(table);
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new TrinoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to create Accumulo table", e);
        }
        catch (TableExistsException e) {
            throw new TrinoException(ACCUMULO_TABLE_EXISTS, "Accumulo table already exists", e);
        }
    }

    public void setLocalityGroups(String tableName, Map<String, Set<Text>> groups)
    {
        if (groups.isEmpty()) {
            return;
        }

        try {
            client.tableOperations().setLocalityGroups(tableName, groups);
            LOG.debug("Set locality groups for %s to %s", tableName, groups);
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new TrinoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to set locality groups", e);
        }
        catch (TableNotFoundException e) {
            throw new TrinoException(ACCUMULO_TABLE_DNE, "Failed to set locality groups, table does not exist", e);
        }
    }

    public void setIterator(String table, IteratorSetting setting)
    {
        try {
            // Remove any existing iterator settings of the same name, if applicable
            Map<String, EnumSet<IteratorScope>> iterators = client.tableOperations().listIterators(table);
            if (iterators.containsKey(setting.getName())) {
                client.tableOperations().removeIterator(table, setting.getName(), iterators.get(setting.getName()));
            }

            client.tableOperations().attachIterator(table, setting);
        }
        catch (AccumuloSecurityException | AccumuloException e) {
            throw new TrinoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to set iterator on table " + table, e);
        }
        catch (TableNotFoundException e) {
            throw new TrinoException(ACCUMULO_TABLE_DNE, "Failed to set iterator, table does not exist", e);
        }
    }

    public void deleteAccumuloTable(String tableName)
    {
        try {
            client.tableOperations().delete(tableName);
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new TrinoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to delete Accumulo table", e);
        }
        catch (TableNotFoundException e) {
            throw new TrinoException(ACCUMULO_TABLE_DNE, "Failed to delete Accumulo table, does not exist", e);
        }
    }

    public void renameAccumuloTable(String oldName, String newName)
    {
        try {
            client.tableOperations().rename(oldName, newName);
        }
        catch (AccumuloSecurityException | AccumuloException e) {
            throw new TrinoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to rename table", e);
        }
        catch (TableNotFoundException e) {
            throw new TrinoException(ACCUMULO_TABLE_DNE, "Failed to rename table, old table does not exist", e);
        }
        catch (TableExistsException e) {
            throw new TrinoException(ACCUMULO_TABLE_EXISTS, "Failed to rename table, new table already exists", e);
        }
    }
}
