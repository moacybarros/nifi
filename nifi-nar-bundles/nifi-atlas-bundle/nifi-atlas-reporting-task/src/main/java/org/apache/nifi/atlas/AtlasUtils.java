package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasObjectId;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

public class AtlasUtils {

    public static String toStr(Object obj) {
        return obj != null ? obj.toString() : null;
    }


    public static boolean isGuidAssigned(String guid) {
        return guid != null && !guid.startsWith("-");
    }

    public static String toQualifiedName(String componentId, String clusterName) {
        return componentId + "@" + clusterName;
    }

    public static String getComponentIdFromQualifiedName(String qualifiedName) {
        return qualifiedName.split("@")[0];
    }

    public static String getClusterNameFromQualifiedName(String qualifiedName) {
        return qualifiedName.split("@")[1];
    }

    public static boolean isUpdated(Object current, Object arg) {
        if (current == null) {
            // Null to something.
            return arg != null;
        }

        // Something to something.
        return !current.equals(arg);
    }

    public static void updateMetadata(AtomicBoolean updatedTracker, List<String> updateAudit,
                                      String subject, Object currentValue, Object newValue) {
        if (isUpdated(currentValue, newValue)) {
            updatedTracker.set(true);
            updateAudit.add(String.format("%s changed from %s to %s", subject, currentValue, newValue));
        }
    }

    public static Optional<AtlasObjectId> findIdByQualifiedName(Set<AtlasObjectId> ids, String qualifiedName) {
        return ids.stream().filter(id -> qualifiedName.equals(id.getUniqueAttributes().get(ATTR_QUALIFIED_NAME))).findFirst();
    }

}
