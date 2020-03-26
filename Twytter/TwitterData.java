package Twytter;

import java.io.Serializable;
import java.util.Date;

public class TwitterData implements Serializable {

    String Text;
    String User;
    String Place;
    String createdAt;

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }



    public String getText() {
        return Text;
    }

    public void setText(String text) {
        Text = text;
    }

    public String getUser() {
        return User;
    }

    public void setUser(String user) {
        User = user;
    }

    public String getPlace() {
        return Place;
    }

    public void setPlace(String place) {
        Place = place;
    }
}
