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
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.Map;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
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
                .setCaseInsensitiveNameMatching(false)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, MINUTES))
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
                .put("case-insensitive-name-matching", "true")
                .put("case-insensitive-name-matching.cache-ttl", "1s")
                .put("types-mapped-to-varchar-include", "OTHER,ARRAY,mytype,struct_type1,atype")
                .put("types-mapped-to-varchar-exclude", "other_type1,_type1,STRUCT,atype")
                .build();

        BaseJdbcConfig expected = new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setConnectionUser("user")
                .setConnectionPassword("password")
                .setUserCredentialName("foo")
                .setPasswordCredentialName("bar")
                .setCaseInsensitiveNameMatching(true)
                .setCaseInsensitiveNameMatchingCacheTtl(new Duration(1, SECONDS))
                .setTypesMappedToVarcharInclude("OTHER,ARRAY,mytype,struct_type1,atype")
                .setTypesMappedToVarcharExclude("other_type1,_type1,STRUCT,atype");

        ConfigAssertions.assertFullMapping(properties, expected);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.OTHER, Optional.ofNullable("other_type1"), 0, 0, Optional.empty())),
                false);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.OTHER, Optional.ofNullable("other_type2"), 0, 0, Optional.empty())),
                true);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.ARRAY, Optional.ofNullable("_type1"), 0, 0, Optional.empty())),
                false);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.ARRAY, Optional.ofNullable("_type2"), 0, 0, Optional.empty())),
                true);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.OTHER, Optional.ofNullable("mytype"), 0, 0, Optional.empty())),
                true);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.BIGINT, Optional.ofNullable("bigint"), 20, 0, Optional.empty())),
                false);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.TIMESTAMP, Optional.ofNullable("timestamp"), 0, 0, Optional.empty())),
                false);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.STRUCT, Optional.ofNullable("struct_type1"), 0, 0, Optional.empty())),
                true);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.STRUCT, Optional.ofNullable("struct_type2"), 0, 0, Optional.empty())),
                false);

        assertEquals(expected.isTypeMappedToVarchar(new JdbcTypeHandle(Types.OTHER, Optional.ofNullable("atype"), 0, 0, Optional.empty())),
                false);
    }
}
