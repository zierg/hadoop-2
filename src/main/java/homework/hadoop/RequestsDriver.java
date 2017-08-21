package homework.hadoop;

import eu.bitwalker.useragentutils.UserAgent;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class RequestsDriver {

    public static void main(String[] args) {


        String logRecord = "ip2 - - [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/pdf.gif HTTP/1.1\" 200 390 \"http://host2/sun_ss5/\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";
        String[] strings = logRecord.replaceAll("(.+) - -.*\\[.+] \"[^\"]+\" \\d+ (\\d+) \"[^\"]+\" \"([^\"]+)\"", "$1;$2;$3").split(";");
        log.info("Strings: {}", Arrays.toString(strings));

        UserAgent userAgent = UserAgent.parseUserAgentString(strings[2]);
        log.info("Browser: {}", userAgent.getBrowser().getGroup().getName());
    }

    static Logger log = LoggerFactory.getLogger(RequestsDriver.class);
}
