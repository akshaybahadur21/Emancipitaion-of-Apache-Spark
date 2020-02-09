package rapido;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;

public class RapidoTransformedData implements Serializable {

    private DateTime ts;

    public String number;

    public String pick_lat;

    public String pick_lng;

    public String drop_lat;

    public String drop_lng;

    public String pickHex;

    public String dropHex;

    public RapidoTransformedData() {

    }

    public String getPickHex() {
        return pickHex;
    }

    public void setPickHex(String pickHex) {
        this.pickHex = pickHex;
    }

    public String getDropHex() {
        return dropHex;
    }

    public void setDropHex(String dropHex) {
        this.dropHex = dropHex;
    }

    public RapidoTransformedData(String time, String number, String pick_lat, String pick_lng, String drop_lat, String drop_lng, String pickHex, String dropHex) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-DD HH:mm:ss");
        this.ts = formatter.parseDateTime(time);
        this.number = number;
        this.pick_lat = pick_lat;
        this.pick_lng = pick_lng;
        this.drop_lat = drop_lat;
        this.drop_lng = drop_lng;

    }

    public void setTs(String time){
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-DD HH:mm:ss");
        this.ts = formatter.parseDateTime(time);
    }
    public String getTs(){
        return this.ts.toString();
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

