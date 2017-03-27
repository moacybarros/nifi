package org.apache.nifi.atlas.processors;

import org.junit.Test;

import static org.apache.nifi.atlas.processors.ExpressionUtils.isExpressionLanguagePresent;
import static org.junit.Assert.assertEquals;

public class TestExpressionUtils {

    @Test
    public void testIsExpressionPresent() {
        assertEquals(false, isExpressionLanguagePresent("not-present"));
        assertEquals(true, isExpressionLanguagePresent("${now()}"));
        assertEquals(true, isExpressionLanguagePresent("Use ${attribute-name} inside."));
    }

}
