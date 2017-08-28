package org.apache.nifi.atlas.provenance;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RegexClusterResolver implements ClusterResolver {

    public static final String PATTERN_PROPERTY_PREFIX = "clusterNamePattern.";
    private Map<String, Set<Pattern>> clusterNamePatterns;

    @Override
    public Collection<ValidationResult> validate(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>();
        consumeConfigurations(validationContext.getAllProperties(),
                (clusterNamePatterns, patterns) -> {},
                (entry, e) -> {
                    final ValidationResult result = new ValidationResult.Builder()
                            .subject(entry.getKey())
                            .input(entry.getValue())
                            .explanation(e.getMessage())
                            .valid(false)
                            .build();
                    validationResults.add(result);
                });
        return validationResults;
    }

    @Override
    public void configure(PropertyContext context) {

        clusterNamePatterns = new HashMap<>();
        consumeConfigurations(context.getAllProperties(),
                (clusterName, patterns) -> clusterNamePatterns.put(clusterName, patterns),
                null);

    }

    private void consumeConfigurations(final Map<String, String> allProperties,
                                               final BiConsumer<String, Set<Pattern>> consumer,
                                               final BiConsumer<Map.Entry<String, String>, RuntimeException> errorHandler) {
        allProperties.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(PATTERN_PROPERTY_PREFIX))
                .forEach(entry -> {
                    final String clusterName;
                    final Set<Pattern> patterns;
                    try {
                        clusterName = entry.getKey().substring(PATTERN_PROPERTY_PREFIX.length());
                        final String[] regexsArray = entry.getValue().split("\\s");
                        final List<String> regexs = Arrays.stream(regexsArray)
                                .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
                        patterns = parseClusterNamePatterns(clusterName, regexs);
                        consumer.accept(clusterName, patterns);
                    } catch (RuntimeException e) {
                        if (errorHandler != null) {
                            errorHandler.accept(entry, e);
                        } else {
                            throw e;
                        }
                    }
                });
    }

    private Set<Pattern> parseClusterNamePatterns(final String clusterName, List<String> regexs) {
        if (clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("Empty cluster name is not allowed.");
        }

        if (regexs.size() == 0) {
            throw new IllegalArgumentException(
                    String.format("At least one cluster name pattern is required, [%s].", clusterName));
        }

        return regexs.stream().map(Pattern::compile).collect(Collectors.toSet());
    }

    @Override
    public String toClusterName(String hostname) {
        for (Map.Entry<String, Set<Pattern>> entry : clusterNamePatterns.entrySet()) {
            for (Pattern pattern : entry.getValue()) {
                if (pattern.matcher(hostname).matches()) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }



}
