package org.apache.nifi.atlas.provenance.lineage;

import org.apache.atlas.notification.hook.HookNotification;

public interface LineageContext {
    void addMessage(HookNotification.HookNotificationMessage message);
}
