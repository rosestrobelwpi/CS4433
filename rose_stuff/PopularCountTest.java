package org.example;

import org.junit.Test;

import static org.junit.Assert.*;

public class PopularCountTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[3];
        input[0] = "/Users/rosestrobel/shared_folder/friends.csv";
        input[1] = "/Users/rosestrobel/Downloads/temp";
        input[2] = "/Users/rosestrobel/Downloads/out";

        PopularCount pc = new PopularCount();
        pc.debug(input);
    }
}