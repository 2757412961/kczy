package org.dde.zju.kczy.proto;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

import static org.dde.zju.kczy.proto.Constant.EXTENT_SEPORATOR;

@Getter
@Setter
public class Extent implements Serializable {

    double minx;
    double maxx;
    double miny;
    double maxy;

    public Extent() {
    }

    public Extent(String in) {
        String[] extents = in.split(EXTENT_SEPORATOR);
        this.minx = Double.parseDouble(extents[0]);
        this.maxx = Double.parseDouble(extents[1]);
        this.miny = Double.parseDouble(extents[2]);
        this.maxy = Double.parseDouble(extents[3]);
    }


    public Extent(double minx, double maxx, double miny, double maxy) {
        this.minx = minx;
        this.maxx = maxx;
        this.miny = miny;
        this.maxy = maxy;
    }

    /**
     * pare every corder to get the larger one
     * @param in
     * @return
     */
    public Extent add(Extent in) {
        Extent out = new Extent();
        out.setMaxx(Math.max(this.maxx, in.maxx));
        out.setMaxy(Math.max(this.maxy, in.maxy));
        out.setMinx(Math.min(this.minx, in.minx));
        out.setMiny(Math.min(this.miny, in.miny));
        return out;
    }

    public String toShellString() {
        return (new StringBuilder())
                .append(minx).append(EXTENT_SEPORATOR)
                .append(maxx).append(EXTENT_SEPORATOR)
                .append(maxy).append(EXTENT_SEPORATOR)
                .append(miny).toString();
    }

    @Override
    public String toString() {
        return (new StringBuilder())
                .append(minx).append(EXTENT_SEPORATOR)
                .append(maxx).append(EXTENT_SEPORATOR)
                .append(miny).append(EXTENT_SEPORATOR)
                .append(maxy).toString();
    }
}
