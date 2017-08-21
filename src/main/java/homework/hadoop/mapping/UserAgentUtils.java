package homework.hadoop.mapping;

import eu.bitwalker.useragentutils.UserAgent;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
final class UserAgentUtils {

    static String getBrowser(String userAgent) {
        UserAgent agent = UserAgent.parseUserAgentString(userAgent);
        return agent.getBrowser().getGroup().getName();
    }

    private UserAgentUtils() {}
}
