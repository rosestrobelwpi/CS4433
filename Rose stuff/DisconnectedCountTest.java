

import org.junit.Test;

import static org.junit.Assert.*;

public class DisconnectedCountTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];
        input[0] = "/Users/rosestrobel/shared_folder/access_logs.csv";
        input[1] = "/Users/rosestrobel/shared_folder/pages.csv";
        input[2] = "/Users/rosestrobel/Downloads/out";

        DisconnectedCount dc = new DisconnectedCount();
        dc.debug(input);
    }
}