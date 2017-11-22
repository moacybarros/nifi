package org.apache.nifi.atlas.security;

import org.apache.atlas.AtlasClientV2;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.util.StringUtils;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.atlas.reporting.AtlasNiFiFlowLineage.ATLAS_PASSWORD;
import static org.apache.nifi.atlas.reporting.AtlasNiFiFlowLineage.ATLAS_USER;

public class Basic implements AtlasAuthN {

    private String user;
    private String password;

    @Override
    public Collection<ValidationResult> validate(ValidationContext context) {
        return Stream.of(
                validateRequiredField(context, ATLAS_USER),
                validateRequiredField(context, ATLAS_PASSWORD)
        ).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    }

    @Override
    public void configure(PropertyContext context) {
        user = context.getProperty(ATLAS_USER).evaluateAttributeExpressions().getValue();
        password = context.getProperty(ATLAS_PASSWORD).evaluateAttributeExpressions().getValue();

        if (StringUtils.isEmpty(user)) {
            throw new IllegalArgumentException("User is required for basic auth.");
        }

        if (StringUtils.isEmpty(password)){
            throw new IllegalArgumentException("Password is required for basic auth.");
        }
    }

    @Override
    public AtlasClientV2 createClient(String[] baseUrls) {
        return new AtlasClientV2(baseUrls, new String[]{user, password});
    }
}
