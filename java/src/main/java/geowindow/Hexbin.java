package geowindow;

/**
 * Created by tom on 7/2/16.
 *
 * Normalize points to their hexbin centers
 *
 * Adapted from d3.hexbin: https://github.com/d3/d3-hexbin/blob/master/src/hexbin.js
 */

public class Hexbin {
    private static final double thirdPi = Math.PI/3.0;

    private double dx;
    private double dy;

    public Hexbin(double radius) {
        this.dx = 2 * radius * Math.sin(thirdPi);
        this.dy = 1.5 * radius;
    }

    public double[] bin(double x, double y) {
        double py = y/dy;
        double pj = Math.round(py);
        double px = x/dx - ( (int)pj & 1) / 2.0;
        double pi = Math.round(px);
        double py1 = py - pj;

        if (3*Math.abs(py1) > 1) {
            double px1 = px - pi;
            double pi2 = pi + (px < pi ? -1 : 1) / 2.0;
            double pj2 = pj + (py < pj ? -1 : 1);
            double px2 = px - pi2;
            double py2 = py - pj2;

            if (px1*px1 + py1*py1 > px2*px2 + py2*py2) {
                pi = pi2 + ( ( (int)pj & 1) != 0 ? 1 : -1) / 2.0;
                pj = pj2;
            }
        }
        return new double[] {
                (pi + ( (int)pj & 1) / 2.0) * dx,
                pj * dy
        };
    }
}
