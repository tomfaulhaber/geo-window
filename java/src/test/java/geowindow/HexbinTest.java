package geowindow;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by tom on 7/5/16.
 */
public class HexbinTest {
    @Test
    public void bin() throws Exception {
        Hexbin hb = new Hexbin(1.0/240.0);
        double arg[] = {-122.40793609,37.79038645};
        double expected[] = {-122.40908238574796, 37.793750};
        double[] result = hb.bin(arg[0], arg[1]);

        assertArrayEquals(expected, result, 0.00001);
    }

}