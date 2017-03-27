package org.apache.nifi.atlas.processors;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class ExpressionUtils {

    private static final Pattern hasEl = Pattern.compile(".*\\$\\{.+}.*");

    public static boolean isExpressionLanguagePresent(String value) {
        if(isBlank(value)) {
            return false;
        }

        final Matcher matcher = hasEl.matcher(value);
        return matcher.matches();
    }

}
