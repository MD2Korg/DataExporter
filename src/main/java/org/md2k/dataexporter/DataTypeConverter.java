package org.md2k.dataexporter;/*
 * Copyright (c) 2015, The University of Memphis, MD2K Center 
 * - Timothy Hnat <twhnat@memphis.edu>
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * 
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import com.google.gson.JsonArray;
import org.md2k.datakitapi.datatype.*;

public class DataTypeConverter {
    public static String dataTypeToString(DataType dt) {
        String temp = "";
        if (dt instanceof DataTypeBoolean) {
            temp = Long.toString(dt.getDateTime());
            temp += ", " + ((DataTypeBoolean) dt).getSample();
        } else if (dt instanceof DataTypeBooleanArray) {
            temp = Long.toString(dt.getDateTime());
            for (Boolean d : ((DataTypeBooleanArray) dt).getSample()) {
                temp += ", " + d;
            }
        } else if (dt instanceof DataTypeJSONObject) {
            temp = Long.toString(dt.getDateTime());
            temp += ", " + ((DataTypeJSONObject) dt).getSample().toString();
        } else if (dt instanceof DataTypeJSONObjectArray) {
            temp = Long.toString(dt.getDateTime());
            temp += ", " + ((DataTypeJSONObjectArray) dt).getSample().toString();
        } else if (dt instanceof DataTypeByte) {
            temp = Long.toString(dt.getDateTime());
            temp += ", " + ((DataTypeByte) dt).getSample();
        } else if (dt instanceof DataTypeByteArray) {
            temp = Long.toString(dt.getDateTime());
            for (Byte d : ((DataTypeByteArray) dt).getSample()) {
                temp += ", " + d;
            }
        } else if (dt instanceof DataTypeDouble) {
            temp = Long.toString(dt.getDateTime());
            temp += ", " + ((DataTypeDouble) dt).getSample();
        } else if (dt instanceof DataTypeDoubleArray) {
            temp = Long.toString(dt.getDateTime());
            for (Double d : ((DataTypeDoubleArray) dt).getSample()) {
                temp += ", " + d;
            }
        } else if (dt instanceof DataTypeFloat) {
            temp = Long.toString(dt.getDateTime());
            temp += ", " + ((DataTypeFloat) dt).getSample();
        } else if (dt instanceof DataTypeFloatArray) {
            temp = Long.toString(dt.getDateTime());
            for (Float d : ((DataTypeFloatArray) dt).getSample()) {
                temp += ", " + d;
            }
        } else if (dt instanceof DataTypeInt) {
            temp = Long.toString(dt.getDateTime());
            temp += ", " + ((DataTypeInt) dt).getSample();
        } else if (dt instanceof DataTypeIntArray) {
            temp = Long.toString(dt.getDateTime());
            for (Integer d : ((DataTypeIntArray) dt).getSample()) {
                temp += ", " + d;
            }
        } else if (dt instanceof DataTypeLong) {
            temp = Long.toString(dt.getDateTime());
            temp += ", " + ((DataTypeLong) dt).getSample();
        } else if (dt instanceof DataTypeLongArray) {
            temp = Long.toString(dt.getDateTime());
            for (Long d : ((DataTypeLongArray) dt).getSample()) {
                temp += ", " + d;
            }
        } else if (dt instanceof DataTypeString) {
            temp = Long.toString(dt.getDateTime());
            temp += ", " + ((DataTypeString) dt).getSample();
        } else if (dt instanceof DataTypeStringArray) {
            temp = Long.toString(dt.getDateTime());
            for (String d : ((DataTypeStringArray) dt).getSample()) {
                temp += ", " + d;
            }
        } else {
            System.out.println("Unknown Object");
        }
        return temp;
    }

    public static JsonArray dataTypeToJSON(DataType dt) {

        JsonArray temp = new JsonArray();
        if (dt instanceof DataTypeBoolean) {
            temp.add(((DataTypeBoolean) dt).getSample());
        } else if (dt instanceof DataTypeBooleanArray) {
            JsonArray da = new JsonArray();
            for (Boolean d : ((DataTypeBooleanArray) dt).getSample()) {
                da.add(d);
            }
            da.add(da);

        } else if (dt instanceof DataTypeJSONObject) {
            temp.add(((DataTypeJSONObject) dt).getSample());
        } else if (dt instanceof DataTypeJSONObjectArray) {
            temp.add(((DataTypeJSONObjectArray) dt).getSample());

        } else if (dt instanceof DataTypeByte) {
            temp.add(((DataTypeByte) dt).getSample());
        } else if (dt instanceof DataTypeByteArray) {
            JsonArray da = new JsonArray();
            for (Byte d : ((DataTypeByteArray) dt).getSample()) {
                da.add(d);
            }
            temp.addAll(da);

        } else if (dt instanceof DataTypeDouble) {
            temp.add(((DataTypeDouble) dt).getSample());
        } else if (dt instanceof DataTypeDoubleArray) {
            JsonArray da = new JsonArray();
            for (Double d : ((DataTypeDoubleArray) dt).getSample()) {
                da.add(d);
            }
            temp.addAll(da);

        } else if (dt instanceof DataTypeFloat) {
            temp.add(((DataTypeFloat) dt).getSample());
        } else if (dt instanceof DataTypeFloatArray) {
            JsonArray da = new JsonArray();
            for (Float d : ((DataTypeFloatArray) dt).getSample()) {
                da.add(d);
            }
            temp.addAll(da);

        } else if (dt instanceof DataTypeInt) {
            temp.add(((DataTypeInt) dt).getSample());
        } else if (dt instanceof DataTypeIntArray) {
            JsonArray da = new JsonArray();
            for (Integer d : ((DataTypeIntArray) dt).getSample()) {
                da.add(d);
            }
            temp.addAll(da);

        } else if (dt instanceof DataTypeLong) {
            temp.add(((DataTypeLong) dt).getSample());
        } else if (dt instanceof DataTypeLongArray) {
            JsonArray da = new JsonArray();
            for (Long d : ((DataTypeLongArray) dt).getSample()) {
                da.add(d);
            }
            temp.addAll(da);

        } else if (dt instanceof DataTypeString) {
            temp.add(((DataTypeString) dt).getSample());
        } else if (dt instanceof DataTypeStringArray) {
            JsonArray da = new JsonArray();
            for (String d : ((DataTypeStringArray) dt).getSample()) {
                da.add(d);
            }
            temp.addAll(da);

        } else {
            System.out.println("Unknown Object");
        }
        return temp;


    }
}
