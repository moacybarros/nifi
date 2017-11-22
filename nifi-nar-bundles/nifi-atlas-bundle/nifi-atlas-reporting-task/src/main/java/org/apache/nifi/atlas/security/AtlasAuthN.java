package org.apache.nifi.atlas.security;

import org.apache.atlas.AtlasClientV2;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

public interface AtlasAuthN {
    AtlasClientV2 createClient(final String[] baseUrls);
    Collection<ValidationResult> validate(final ValidationContext context);
    void configure(final PropertyContext context);

    /**
     * Populate required Atlas application properties.
     * This method is called when Atlas reporting task generates atlas-application.properties.
     */
    default void populateProperties(final Properties properties){};

    default Optional<ValidationResult> validateRequiredField(ValidationContext context, PropertyDescriptor prop) {
        if (!context.getProperty(prop).isSet()) {
            return Optional.of(new ValidationResult.Builder()
                    .subject(prop.getDisplayName())
                    .valid(false)
                    .explanation(String.format("required by '%s' auth.", this.getClass().getSimpleName()))
                    .build());
        }
        return Optional.empty();
    }
}
