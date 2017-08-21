package homework.hadoop.mapping;

import eu.bitwalker.useragentutils.UserAgent;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

@SuppressWarnings("WeakerAccess")
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public final class UserAgentUtils {

    public static String getBrowser(String userAgent) {
        UserAgent agent = UserAgent.parseUserAgentString(userAgent);
        return agent.getBrowser().getGroup().getName();
    }

    private UserAgentUtils() {}
}
