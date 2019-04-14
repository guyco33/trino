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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestBaseJdbcConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(BaseJdbcConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setUserCredentialName(null)
                .setPasswordCredentialName(null)
                .setTypesMappedToVarcharInclude(null)
                .setTypesMappedToVarcharExclude(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("connection-url", "jdbc:h2:mem:config")
                .put("connection-user", "user")
                .put("connection-password", "password")
                .put("user-credential-name", "foo")
                .put("password-credential-name", "bar")
                .put("types-mapped-to-varchar-include", "OTHER,ARRAY,mytype,struct_type1,atype")
                .put("types-mapped-to-varchar-exclude", "other_type1,_type1,STRUCT,atype")
                .build();

        BaseJdbcConfig expected = new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setConnectionUser("user")
                .setConnectionPassword("password")
                .setUserCredentialName("foo")
                .setPasswordCredentialName("bar")
                .setTypesMappedToVarcharInclude("OTHER,ARRAY,mytype,struct_type1,atype")
                .setTypesMappedToVarcharExclude("other_type1,_type1,STRUCT,atype");

        ConfigAssertions.assertFullMapping(properties, expected);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.OTHER, "other_type1", 0, 0)),
                false);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.OTHER, "other_type2", 0, 0)),
                true);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.ARRAY, "_type1", 0, 0)),
                false);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.ARRAY, "_type2", 0, 0)),
                true);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.OTHER, "mytype", 0, 0)),
                true);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.BIGINT, "bigint", 20, 0)),
                false);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.TIMESTAMP, "timestamp", 0, 0)),
                false);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.STRUCT, "struct_type1", 0, 0)),
                true);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.STRUCT, "struct_type2", 0, 0)),
                false);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.OTHER, "atype", 0, 0)),
                false);
    }
}
