package io.indexr.segment.rc;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.Collection;
import java.util.Collections;

abstract class ColCmpVal implements CmpOperator {
    @JsonProperty("attr")
    public final Attr attr;
    @JsonProperty("numValue")
    public final long numValue;
    @JsonIgnore
    public final UTF8String strValue;

    @JsonProperty("strValue")
    public String getStrValue() {return strValue == null ? null : strValue.toString();}

    public ColCmpVal(Attr attr,
                     long numValue,
                     String strValue) {
        this(attr, numValue, strValue == null ? null : UTF8String.fromString(strValue));
    }

    public ColCmpVal(Attr attr,
                     long numValue,
                     UTF8String strValue) {
        this.attr = attr;
        this.numValue = numValue;
        this.strValue = strValue;
    }

    @Override
    public Collection<Attr> attr() {
        return Collections.singleton(attr);
    }

    @Override
    public String toString() {
        if (strValue != null) {
            return String.format("%s($%s: %s)", this.getClass().getSimpleName(), attr, strValue);
        } else {
            return String.format("%s($%s: %s)", this.getClass().getSimpleName(), attr, numValue);
        }
    }
}
