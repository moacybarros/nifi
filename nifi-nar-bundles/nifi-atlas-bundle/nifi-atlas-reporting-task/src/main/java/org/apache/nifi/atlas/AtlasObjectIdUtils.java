package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.atlas.processors.ExpressionUtils;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static org.apache.nifi.atlas.processors.ExpressionUtils.isExpressionLanguagePresent;

public class AtlasObjectIdUtils {

    // Condition of a valid AtlasObjectId:
    // It has a GUID, or It has a type name and unique attributes, and none of attribute contains ExpressionLanguage.
    public static final Predicate<String> notEmpty = s -> s != null && !s.isEmpty();
    public static final Predicate<AtlasObjectId> validObjectId = id -> notEmpty.test(id.getGuid())
            || (notEmpty.test(id.getTypeName())
                && (id.getUniqueAttributes() != null
                    && !id.getUniqueAttributes().isEmpty()
                    && id.getUniqueAttributes().values().stream().filter(Objects::nonNull)
                        .noneMatch(o -> isExpressionLanguagePresent(o.toString()))));

}
