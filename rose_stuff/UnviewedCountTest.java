package org.example;

import org.junit.Test;

import static org.junit.Assert.*;

public class UnviewedCountTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[4];
        input[0] = "/Users/rosestrobel/shared_folder/friends.csv";
        input[1] = "/Users/rosestrobel/shared_folder/access_logs.csv";
        input[2] = "/Users/rosestrobel/shared_folder/pages.csv";
        input[3] = "/Users/rosestrobel/Downloads/out";

        UnviewedCount uc = new UnviewedCount();
        uc.debug(input);
    }
}