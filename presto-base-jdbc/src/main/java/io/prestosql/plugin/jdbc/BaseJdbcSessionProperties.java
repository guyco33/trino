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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.math.RoundingMode;
import java.util.List;

import static io.prestosql.spi.session.PropertyMetadata.enumProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;

public final class BaseJdbcSessionProperties
        implements SessionPropertiesProvider
{
    private static final String DECIMAL_DEFAULT_SCALE = "decimal_default_scale";
    private static final String DECIMAL_ROUNDING_MODE = "decimal_rounding_mode";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public BaseJdbcSessionProperties(BaseJdbcConfig baseJdbcConfig)
    {
        sessionProperties = ImmutableList.of(
                integerProperty(
                        DECIMAL_DEFAULT_SCALE,
                        "Default scale to use when decimal type scale is unknown",
                        baseJdbcConfig.getDecimalDefaultScale(),
                        false),
                enumProperty(
                        DECIMAL_ROUNDING_MODE,
                        "Decimal rounding mode to use when needed",
                        RoundingMode.class,
                        baseJdbcConfig.getDecimalRoundingMode(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Integer getDecimalDefaultScale(ConnectorSession session)
    {
        return session.getProperty(DECIMAL_DEFAULT_SCALE, Integer.class);
    }

    public static RoundingMode getDecimalRoundingMode(ConnectorSession session)
    {
        return session.getProperty(DECIMAL_ROUNDING_MODE, RoundingMode.class);
    }
}
