package utils;

import java.util.Comparator;

public class UniqueComparator implements Comparator<Unique> {


    public int compare(Unique uk1, Unique uk2) {
        if (uk1.getKey() > uk2.getKey()) {
            return 1;
        } else
            return -1;
    }
}