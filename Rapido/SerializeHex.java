package rapido;

import com.uber.h3core.H3Core;

import java.io.IOException;
import java.io.Serializable;

public class SerializeHex implements Serializable {

    public static String getSerializeHex(String lat, String lng, int res) throws IOException {
        H3Core h3 = H3Core.newInstance();
        return h3.geoToH3Address(Double.parseDouble(lat), Double.parseDouble(lng), res);
    }
}
