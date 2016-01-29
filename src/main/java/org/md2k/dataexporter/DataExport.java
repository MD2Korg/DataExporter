package org.md2k.dataexporter;

/*
 * Copyright (c) 2016, The University of Memphis, MD2K Center
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.md2k.cerebralcortex.CerebralCortexDataPackage;
import org.md2k.cerebralcortex.StudyInfo;
import org.md2k.cerebralcortex.TSV;
import org.md2k.cerebralcortex.UserInfo;
import org.md2k.datakitapi.datatype.*;
import org.md2k.datakitapi.source.datasource.DataSource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.zip.GZIPOutputStream;


/**
 * Data Export tool
 */
public class DataExport {

    private Statement statement = null;

    /**
     * Build a DataExport object that connects to a sqlite database file
     *
     * @param filename SQLite database file
     */
    public DataExport(String filename) {
        try {
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + filename);
            statement = connection.createStatement();
            statement.setQueryTimeout(60);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    public DataExport() {

    }


    /**
     * Generate JSON from query results
     *
     * @param ds     DataSource object from DataKit API
     * @param result JSON string representation of the CerebralCortexDataPackage object
     * @return
     */
    private String generateJSON(UserInfo userInfo, StudyInfo studyInfo, DataSource ds, List<String> result) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        CerebralCortexDataPackage obj = new CerebralCortexDataPackage();

        obj.datasource = ds;
        obj.userinfo = userInfo;
        obj.studyinfo = studyInfo;

        for (String s : result) {
            obj.data.add(new TSV(s));
        }
        return gson.toJson(obj);
    }

    /**
     * Generate JSON from query results
     *
     * @param ds     DataSource object from DataKit API
     * @param result JSON string representation of the CerebralCortexDataPackage object
     * @return
     */
    private String generateJSON(DataSource ds, List<String> result) {
        UserInfo userInfo = getUserInfo();
        StudyInfo studyInfo = getStudyInfo();
        return generateJSON(userInfo, studyInfo, ds, result);
    }

    /**
     * Generate and write a data stream to file in the JSON format
     *
     * @param id Datastream id
     */
    public void writeJSONDataFile(Integer id) {
        try {
            String filename = getOutputFilename(id);
            List<String> result = getTimeseriesDataStream(id);
            DataSource ds = getDataSource(id);

            String json = generateJSON(ds, result);
            Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename + ".json", false), "utf-8"));
            writer.write(json);
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Generate a Gzipped JSON byte array
     *
     * @param id Datastream id
     * @return byte array representing a Gzipped JSON representation of the CerebralCortexDataPackage object
     */
    private byte[] generateGzipJSON(Integer id) {
        List<String> result = getTimeseriesDataStream(id);
        DataSource ds = getDataSource(id);
        UserInfo ui = getUserInfo();
        StudyInfo si = getStudyInfo();
        return generateGzipJSON(result, ui, si, ds);
    }

    /**
     * Generate a Gzipped JSON byte array
     *
     * @param result List of data items
     * @param ds     DataSource object
     * @return byte array representing a Gzipped JSON representation of the CerebralCortexDataPackage object
     */
    private byte[] generateGzipJSON(List<String> result, UserInfo ui, StudyInfo si, DataSource ds) {
        String json = generateJSON(ui, si, ds, result);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            GZIPOutputStream gzip = new GZIPOutputStream(bos);
            OutputStreamWriter osw = new OutputStreamWriter(gzip, StandardCharsets.UTF_8);
            osw.write(json);
            osw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return bos.toByteArray();

    }

    /**
     * Generate and write a data stream to a Gzip-compressed file in the JSON format
     *
     * @param id Datastream id
     */
    public void writeGzipJSONDataFile(Integer id) {
        try {
            String filename = getOutputFilename(id);
            List<String> result = getTimeseriesDataStream(id);
            DataSource ds = getDataSource(id);

            String json = generateJSON(ds, result);

            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filename + ".gz", false));
            GZIPOutputStream gzip = new GZIPOutputStream(bos);
            OutputStreamWriter osw = new OutputStreamWriter(gzip, StandardCharsets.UTF_8);
            osw.write(json);
            osw.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Generate and write a data stream to a CSV file.  Note: meta data that is in the JSON representations is not
     * present in the CSV files.
     *
     * @param id Datastream id
     */
    public void writeCSVDataFile(Integer id) {
        try {
            String filename = getOutputFilename(id);
            List<String> result = getTimeseriesDataStream(id);

            Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename + ".csv", false), "utf-8"));
            for (String s : result) {
                writer.write(s + "\n");
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Retrieve the user id from its data stream
     *
     * @return populated UserInfo object
     */
    private UserInfo getUserInfo() {
        UserInfo result = new UserInfo();
        int streamID = -1;
        try {
            ResultSet rs = statement.executeQuery("select ds_id from datasource where datasource.datasource_type=='USER_INFO'");
            while (rs.next()) {
                streamID = rs.getInt("ds_id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (streamID >= 0) {
            List<String> user = getTimeseriesDataStream(streamID);
            for (String s : user) {
                String[] json = s.split(",", 2);
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                UserInfo ui = gson.fromJson(json[1], UserInfo.class);
                if (!ui.user_id.isEmpty()) {
                    return ui;
                }
            }
        }
        return result;
    }

    /**
     * Retrieve the study id and study name from its data stream
     *
     * @return populated StudyInfo object
     */
    private StudyInfo getStudyInfo() {
        StudyInfo result = new StudyInfo();
        int streamID = -1;
        try {
            ResultSet rs = statement.executeQuery("select ds_id from datasource where datasource.datasource_type=='STUDY_INFO'");
            while (rs.next()) {
                streamID = rs.getInt("ds_id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (streamID >= 0) {
            List<String> study = getTimeseriesDataStream(streamID);
            for (String s : study) {
                String[] json = s.split(",", 2);
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                StudyInfo si = gson.fromJson(json[1], StudyInfo.class);
                if (!si.study_id.isEmpty()) {
                    return si;
                }
            }
        }
        return result;
    }


    /**
     * Retrieve datasource ids from the database
     *
     * @return List of ids
     */
    public List<Integer> getIDs() {
        List<Integer> ids = new ArrayList<Integer>();
        try {
            ResultSet rs = statement.executeQuery("Select ds_id from datasource");
            while (rs.next()) {
                ids.add(rs.getInt("ds_id"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return ids;
    }

    /**
     * Generate an output filename based on the datastream id
     *
     * @param id Datastream id
     * @return String identifier for a filename
     */
    private String getOutputFilename(Integer id) {
        String filename = "";
        List<String> parameters = new ArrayList<String>();
        try {
            ResultSet rs = statement.executeQuery("Select ds_id, datasource_type, platform_id, platform_type from datasource where ds_id = " + id);
            while (rs.next()) {
                parameters.add(Integer.toString(rs.getInt("ds_id")));
                parameters.add(rs.getString("datasource_type"));
                parameters.add(rs.getString("platform_id"));
                parameters.add(rs.getString("platform_type"));
            }
            filename = String.join("_", parameters);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return filename;
    }

    /**
     * Get datasource from the database and decode it
     *
     * @param id Datasource id to retrieve
     * @return DataSource object populated from the database
     */
    private DataSource getDataSource(Integer id) {
        DataSource result = null;
        try {
            ResultSet rs = statement.executeQuery("Select datasource from datasource where ds_id = " + id);
            while (rs.next()) {
                byte[] b = rs.getBytes("datasource");
                result = DataSource.fromBytes(b);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    private List<String> getTimeseriesDataStream(List<DataType> data) {
        List<String> result = new ArrayList<String>();
        for (DataType dt : data) {
            result.add(DataTypeToString(dt));
        }

        return result;
    }

    /**
     * Main method to retrieve a timeseries datastream from the database.  It decodes all known encodings and
     * represents them in their appropriate Java object form
     *
     * @param id Stream identifier
     * @return List of string representations of the datastream elements
     */
    private List<String> getTimeseriesDataStream(Integer id) {
        List<String> result = new ArrayList<String>();
        try {
            ResultSet rs = statement.executeQuery("Select sample from data where datasource_id = " + id);
            while (rs.next()) {
                byte[] b = rs.getBytes("sample");
                DataType dt = DataType.fromBytes(b);
                result.add(DataTypeToString(dt));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    private String DataTypeToString(DataType dt) {
        String temp = "";
        if (dt instanceof DataTypeBoolean) {
            temp = Long.toString(dt.getDateTime());
            temp += ", " + ((DataTypeBoolean) dt).getSample();
        } else if (dt instanceof DataTypeBooleanArray) {
            temp = Long.toString(dt.getDateTime());
            for (Boolean d : ((DataTypeBooleanArray) dt).getSample()) {
                temp += ", " + d;
            }
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

    /**
     * Utility function to convert a byte[] to hex
     *
     * @param hash
     * @return
     */
    private static String byteArray2Hex(final byte[] hash) {
        Formatter formatter = new Formatter();
        for (byte b : hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public boolean publishTimeSeriesDataStream(String requestURL, List<DataType> data, UserInfo ui, StudyInfo si, DataSource ds) {
        List<String> datastream = getTimeseriesDataStream(data);
        byte[] bData = generateGzipJSON(datastream, ui, si, ds);
        return publishGzipJSONData(requestURL, bData);
    }

    /**
     * Upload method for publishing data to the Cerebral Cortex webservice
     *
     * @param request URL
     * @param id      of the datastream to publish
     */
    public boolean publishGzipJSONData(String request, Integer id) {
        byte[] data = generateGzipJSON(id);
        return publishGzipJSONData(request, data);
    }

    /**
     * Upload method for publishing data to the Cerebral Cortex webservice
     *
     * @param request URL
     * @param data    Byte[] of data to send to Cerebral Cortex
     */
    private boolean publishGzipJSONData(String request, byte[] data) {
        String hash = null;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            hash = byteArray2Hex(md.digest(data));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return false;
        }

        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(request);

        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        builder.addBinaryBody("file", data, ContentType.DEFAULT_BINARY, "file");
        builder.addTextBody("hash", hash, ContentType.DEFAULT_TEXT);

        HttpEntity entity = builder.build();
        post.setEntity(entity);
        HttpResponse response = null;
        try {
            response = client.execute(post);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        System.out.println(response);
        return true;
    }

}
