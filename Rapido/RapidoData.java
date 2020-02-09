package rapido;

import org.joda.time.DateTime;

import java.io.Serializable;

public class RapidoData implements Serializable {

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    private String ts;

    private String number;

    private String pick_lat;

    private String pick_lng;

    private String drop_lat;

    private String drop_lng;

    public RapidoData(String ts, String number, String pick_lat, String pick_lng, String drop_lat, String drop_lng) {
        this.ts = ts;
        this.number = number;
        this.pick_lat = pick_lat;
        this.pick_lng = pick_lng;
        this.drop_lat = drop_lat;
        this.drop_lng = drop_lng;
    }
    public void setNumber(String number){
        this.number = number;
    }
    public String getNumber(){
        return this.number;
    }
    public void setPick_lat(String pick_lat){
        this.pick_lat = pick_lat;
    }
    public String getPick_lat(){
        return this.pick_lat;
    }
    public void setPick_lng(String pick_lng){
        this.pick_lng = pick_lng;
    }
    public String getPick_lng(){
        return this.pick_lng;
    }
    public void setDrop_lat(String drop_lat){
        this.drop_lat = drop_lat;
    }
    public String getDrop_lat(){
        return this.drop_lat;
    }
    public void setDrop_lng(String drop_lng){
        this.drop_lng = drop_lng;
    }
    public String getDrop_lng(){
        return this.drop_lng;
    }
}

