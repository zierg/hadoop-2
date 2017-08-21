package homework.hadoop.mapping;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;

@SuppressWarnings("WeakerAccess")
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public final class ParamsExtractor {

    /**
     * Parses a log record
     * @param logRecord the log record
     * @return the log record's params in an array in the following form [ip, bytes, user_agent]
     */
    public static String[] getLogRecordParams(String logRecord) {
        return logRecord.replaceAll(MAPPING_REGEX, MAPPING_REPLACEMENT).split(MAPPING_DELIMITER);
    }

    static String MAPPING_REGEX = "(.+) - -.*\\[.+] \"[^\"]+\" \\d+ (-|\\d+) \"[^\"]*\" \"([^\"]+)\"";
    static String MAPPING_DELIMITER = "\\[----]";
    static String MAPPING_REPLACEMENT = "$1" + MAPPING_DELIMITER + "$2" + MAPPING_DELIMITER + "$3";

    private ParamsExtractor() {}
}
