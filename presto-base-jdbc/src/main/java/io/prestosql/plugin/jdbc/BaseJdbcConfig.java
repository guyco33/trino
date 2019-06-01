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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;

public class BaseJdbcConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String userCredentialName;
    private String passwordCredentialName;
    private boolean caseInsensitiveNameMatching;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);
    private String typesMappedToVarcharInclude;
    private String typesMappedToVarcharExclude;
    private Set<Integer> includeTypesMappingToVarchar;
    private Set<String> includeTypeNamesMappingToVarchar;
    private Set<Integer> excludeTypesMappingToVarchar;
    private Set<String> excludeTypeNamesMappingToVarchar;

    private static List<Field> getStatics(Class<?> clazz)
    {
        return Arrays.stream(clazz.getDeclaredFields()).filter(f ->
                Modifier.isStatic(f.getModifiers())).collect(toList());
    }

    private static final Map<String, Integer> typesMap = new HashMap<String, Integer>()
    {
        {
            for (Field f : getStatics(Types.class)) {
                try {
                    put(f.getName(), f.getInt(f));
                }
                catch (Exception e) {
                }
            }
        }
    };

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public BaseJdbcConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @Nullable
    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("connection-user")
    public BaseJdbcConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @Nullable
    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("connection-password")
    @ConfigSecuritySensitive
    public BaseJdbcConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    public String getTypesMappedToVarcharInclude()
    {
        return typesMappedToVarcharInclude;
    }

    @Config("types-mapped-to-varchar-include")
    public BaseJdbcConfig setTypesMappedToVarcharInclude(String typesMappedToVarcharInclude)
    {
        this.typesMappedToVarcharInclude = typesMappedToVarcharInclude;
        this.includeTypesMappingToVarchar = new HashSet<Integer>();
        this.includeTypeNamesMappingToVarchar = new HashSet<String>();
        if (typesMappedToVarcharInclude != null) {
            for (String type : typesMappedToVarcharInclude.split(",")) {
                if (typesMap.containsKey(type.toUpperCase())) {
                    this.includeTypesMappingToVarchar.add(typesMap.get(type.toUpperCase()));
                }
                else {
                    this.includeTypeNamesMappingToVarchar.add(type);
                }
            }
        }
        return this;
    }

    public String getTypesMappedToVarcharExclude()
    {
        return typesMappedToVarcharExclude;
    }

    @Config("types-mapped-to-varchar-exclude")
    public BaseJdbcConfig setTypesMappedToVarcharExclude(String typesMappedToVarcharExclude)
    {
        this.typesMappedToVarcharExclude = typesMappedToVarcharExclude;
        this.excludeTypesMappingToVarchar = new HashSet<Integer>();
        this.excludeTypeNamesMappingToVarchar = new HashSet<String>();
        if (typesMappedToVarcharExclude != null) {
            for (String type : typesMappedToVarcharExclude.split(",")) {
                if (typesMap.containsKey(type.toUpperCase())) {
                    this.excludeTypesMappingToVarchar.add(typesMap.get(type.toUpperCase()));
                }
                else {
                    this.excludeTypeNamesMappingToVarchar.add(type);
                }
            }
        }
        return this;
    }

    public boolean isIncludedTypeNameMappedToVarchar(Optional<String> typeName)
    {
        return typeName.isPresent() && getTypesMappedToVarcharInclude() != null && this.includeTypeNamesMappingToVarchar.contains(typeName.get());
    }

    public boolean isExcludedTypeNameMappedToVarchar(Optional<String> typeName)
    {
        return typeName.isPresent() && getTypesMappedToVarcharExclude() != null && this.excludeTypeNamesMappingToVarchar.contains(typeName.get());
    }

    public boolean isIncludedTypeMappedToVarchar(int type)
    {
        return getTypesMappedToVarcharInclude() != null && this.includeTypesMappingToVarchar.contains(type);
    }

    public boolean isExcludedTypeMappedToVarchar(int type)
    {
        return getTypesMappedToVarcharExclude() != null && this.excludeTypesMappingToVarchar.contains(type);
    }

    public boolean isTypeMappedToVarchar(JdbcTypeHandle typeHandle)
    {
        if (isExcludedTypeNameMappedToVarchar(typeHandle.getJdbcTypeName())) {
            return false;
        }
        if (isIncludedTypeNameMappedToVarchar(typeHandle.getJdbcTypeName())) {
            return true;
        }
        if (isExcludedTypeMappedToVarchar(typeHandle.getJdbcType())) {
            return false;
        }
        return isIncludedTypeMappedToVarchar(typeHandle.getJdbcType());
    }

    @Nullable
    public String getUserCredentialName()
    {
        return userCredentialName;
    }

    @Config("user-credential-name")
    public BaseJdbcConfig setUserCredentialName(String userCredentialName)
    {
        this.userCredentialName = userCredentialName;
        return this;
    }

    @Nullable
    public String getPasswordCredentialName()
    {
        return passwordCredentialName;
    }

    @Config("password-credential-name")
    public BaseJdbcConfig setPasswordCredentialName(String passwordCredentialName)
    {
        this.passwordCredentialName = passwordCredentialName;
        return this;
    }

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Config("case-insensitive-name-matching")
    public BaseJdbcConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl;
    }

    @Config("case-insensitive-name-matching.cache-ttl")
    public BaseJdbcConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }
}
