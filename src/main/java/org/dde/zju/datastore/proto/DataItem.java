package org.dde.zju.datastore.proto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.dde.zju.base.BaseObject;
import org.dde.zju.base.Constant;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class DataItem extends BaseObject implements Serializable {

    private String satellite;
    private String dataUrl;
    private String level;
    private String loadInfo;
    private String resolution;
    private String imageUrl;
    private long timestamp;
    private String imageId;
    private String provider;
    private String nameEn;
    private int bandCount;
    private String sensor;
    private String boundary;
    private double cloud;
//    private String owner;
//    private String privilege;
//    private boolean hasData;

    public DataItem(String str) {
        try {
            this.convertFromStr(str);
        } catch (Exception e) {

        }
    }

}
