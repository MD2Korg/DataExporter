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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
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
import org.md2k.datakitapi.datatype.DataType;
import org.md2k.datakitapi.source.datasource.DataSource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;


/**
 * Data Export tool
 */
public class DataExport {

    public static final int PUBLISH_BUFFER_SIZE = 1000000;
    public static final int SHORT_BUFFER_SIZE = 10;
    public static final int CSV_BUFFER_SIZE = 1000000;
    public static final int JSON_FILE_BUFFER_SIZE = 10000;

    public static final int QUERY_TIMEOUT = 60;

    private Statement statement = null;

    private Kryo kryo = new Kryo();

    /**
     * Build a DataExport object that connects to a sqlite database file
     *
     * @param filename SQLite database file
     */
    public DataExport(String filename) {
        try {
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + filename);
            statement = connection.createStatement();
            statement.setQueryTimeout(QUERY_TIMEOUT);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
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

    /**
     * Generate JSON from query results
     *
     * @param ds     DataSource object from DataKit API
     * @param result JSON string representation of the CerebralCortexDataPackage object
     * @return
     */
    private CerebralCortexDataPackage generateCerebralCortexHeader(UserInfo userInfo, StudyInfo studyInfo, DataSource ds) {
        CerebralCortexDataPackage obj = new CerebralCortexDataPackage();
        obj.datasource = ds;
        obj.userinfo = userInfo;
        obj.studyinfo = studyInfo;

        return obj;
    }

    /**
     * Generate and write a data stream to file in the JSON format
     *
     * @param id Datastream id
     */
    public void writeJSONDataFile(Integer id) {
        try {
            String filename = getOutputFilename(id);
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(new FileOutputStream(filename + ".json", false), "utf-8"));
            writer.setIndent("  ");
            createJSONDataFileRepresentation(id, writer);
            writer.close();

        } catch (Exception e) {
            System.err.println("DataStream ID: " + id);
            e.printStackTrace();
        }
    }

    private void createJSONDataFileRepresentation(Integer id, JsonWriter writer) throws IOException {
        DataSource ds = getDataSource(id);
        UserInfo userInfo = getUserInfo();
        StudyInfo studyInfo = getStudyInfo();

        createJSONHeader(writer, ds, userInfo, studyInfo);
        if (getQueryIDs().contains(id)) {
            SQLiteIterator sqli = new SQLiteIterator(statement, id, JSON_FILE_BUFFER_SIZE);
            createJSONDataRepresentation(writer, sqli, false);
        } else if (getRAWIDs().contains(id)) {
            SQLiteRAWIterator sqli = new SQLiteRAWIterator(statement, id, JSON_FILE_BUFFER_SIZE);
            createJSONDataRepresentation(writer, sqli, false);
        }
        createJSONFooter(writer);
    }

    private void createJSONRAWDataFileRepresentation(Integer id, JsonWriter writer) throws IOException {
        DataSource ds = getDataSource(id);
        UserInfo userInfo = getUserInfo();
        StudyInfo studyInfo = getStudyInfo();

        SQLiteRAWIterator sqliRAW = new SQLiteRAWIterator(statement, id, JSON_FILE_BUFFER_SIZE);
        createJSONHeader(writer, ds, userInfo, studyInfo);
        createJSONDataRepresentation(writer, sqliRAW, false);
        createJSONFooter(writer);
    }

    private void createJSONFooter(JsonWriter writer) throws IOException {
        writer.endArray();
        writer.endObject();
    }

    private boolean createJSONDataRepresentation(JsonWriter writer, Iterator iter, boolean segmentData) throws IOException {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        while(iter.hasNext()) {
            List<DataType> result = (List<DataType>) iter.next();
            System.out.println("Iterator:" + result.size());
            for (DataType dt : result) {
//                TSV entry = new TSV(DataTypeConverter.dataTypeToString(dt));
                TSV entry = new TSV(dt.getDateTime(), DataTypeConverter.dataTypeToJSON(dt));
                gson.toJson(entry, TSV.class, writer);
            }
            if(segmentData) {
                break;
            }
        }

        return iter.hasNext();
    }

    private Gson createJSONHeader(JsonWriter writer, DataSource ds, UserInfo userInfo, StudyInfo studyInfo) throws IOException {
        CerebralCortexDataPackage header = generateCerebralCortexHeader(userInfo, studyInfo, ds);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        writer.beginObject();
        writer.name("userinfo");
        gson.toJson(header.userinfo, UserInfo.class, writer);
        writer.name("studyinfo");
        gson.toJson(header.studyinfo, StudyInfo.class, writer);
        writer.name("datasource");
        gson.toJson(header.datasource, DataSource.class, writer);

        writer.name("data");
        writer.beginArray();
        return gson;
    }

    /**
     * Generate a Gzipped JSON byte array
     *
     * @param result List of data items
     * @param ds     DataSource object
     * @return byte array representing a Gzipped JSON representation of the CerebralCortexDataPackage object
     */
    private ByteOutputArray generateGzipJSON(UserInfo ui, StudyInfo si, DataSource ds, Iterator iter, boolean segmentData) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        boolean additionalData = false;
        try {
            GZIPOutputStream gzip = new GZIPOutputStream(bos);
            OutputStreamWriter osw = new OutputStreamWriter(gzip, StandardCharsets.UTF_8);
            JsonWriter writer = new JsonWriter(osw);
            writer.setIndent("  ");
            createJSONHeader(writer, ds, ui, si);
            additionalData = createJSONDataRepresentation(writer, iter, segmentData);
            createJSONFooter(writer);
            writer.close();
            osw.close();
            gzip.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new ByteOutputArray(bos.toByteArray(), additionalData);
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
            Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename + ".csv", false), "utf-8"));

            if (getQueryIDs().contains(id)) {
                SQLiteIterator sqli = new SQLiteIterator(statement, id, CSV_BUFFER_SIZE);
                while (sqli.hasNext()) {
                    List<DataType> result = sqli.next();
                    System.out.println("Iterator:" + result.size());
                    for (DataType dt : result) {
                        writer.write(DataTypeConverter.dataTypeToString(dt) + "\n");
                    }
                }

            } else if (getRAWIDs().contains(id)) {
                SQLiteRAWIterator sqli = new SQLiteRAWIterator(statement, id, CSV_BUFFER_SIZE);
                while (sqli.hasNext()) {
                    List<DataType> result = sqli.next();
                    System.out.println("Iterator:" + result.size());
                    for (DataType dt : result) {
                        writer.write(DataTypeConverter.dataTypeToString(dt) + "\n");
                    }
                }
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
            ResultSet rs = statement.executeQuery("select ds_id from datasources where datasources.datasource_type=='USER_INFO'");
            while (rs.next()) {
                streamID = rs.getInt("ds_id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (streamID >= 0) {
            SQLiteIterator sqli = new SQLiteIterator(statement, streamID, SHORT_BUFFER_SIZE);
            while(sqli.hasNext()) {
                List<DataType> user = sqli.next();
                for (DataType dt : user) {
                    String[] json = DataTypeConverter.dataTypeToString(dt).split(",", 2);
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    UserInfo ui = gson.fromJson(json[1], UserInfo.class);
                    if (!ui.uuid.isEmpty()) {
                        return ui;
                    }

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
            ResultSet rs = statement.executeQuery("select ds_id from datasources where datasources.datasource_type=='STUDY_INFO'");
            while (rs.next()) {
                streamID = rs.getInt("ds_id");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (streamID >= 0) {
            SQLiteIterator sqli = new SQLiteIterator(statement, streamID, SHORT_BUFFER_SIZE);
            while(sqli.hasNext()) {
                List<DataType> study = sqli.next();
                for (DataType dt : study) {
                    String[] json = DataTypeConverter.dataTypeToString(dt).split(",", 2);
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    StudyInfo si = gson.fromJson(json[1], StudyInfo.class);
                    if (!si.id.isEmpty()) {
                        return si;
                    }

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
            ResultSet rs = statement.executeQuery("Select ds_id from datasources");
            while (rs.next()) {
                ids.add(rs.getInt("ds_id"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ids;
    }

    /**
     * Retrieve datasource ids from the database
     *
     * @return List of ids
     */
    public List<Integer> getQueryIDs() {
        List<Integer> ids = new ArrayList<Integer>();
        try {
            ResultSet rs = statement.executeQuery("Select datasource_id as ds_id from data group by datasource_id");
            while (rs.next()) {
                ids.add(rs.getInt("ds_id"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ids;
    }

    /**
     * Retrieve datasource ids from the database
     *
     * @return List of ids
     */
    public List<Integer> getRAWIDs() {
        List<Integer> ids = new ArrayList<Integer>();
        try {
            ResultSet rs = statement.executeQuery("Select datasource_id as ds_id from rawdata group by datasource_id");
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
            ResultSet rs = statement.executeQuery("Select * from datasources where ds_id = " + id);
            while (rs.next()) {
                parameters.add(Integer.toString(rs.getInt("ds_id")));
                parameters.add(rs.getString("datasource_id"));
                parameters.add(rs.getString("datasource_type"));
                parameters.add(rs.getString("platform_id"));
                parameters.add(rs.getString("platform_type"));
                parameters.add(rs.getString("platformapp_id"));
                parameters.add(rs.getString("platformapp_type"));
                parameters.add(rs.getString("application_id"));
                parameters.add(rs.getString("application_type"));

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
            ResultSet rs = statement.executeQuery("Select datasource from datasources where ds_id = " + id);
            while (rs.next()) {
                byte[] b = rs.getBytes("datasource");
                Input input = new Input(new ByteArrayInputStream(b));
                result = (DataSource) kryo.readClassAndObject(input);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public boolean publishTimeSeriesDataStream(String requestURL, List<DataType> data, UserInfo ui, StudyInfo si, DataSource ds) {
        ListIterator<DataType> iter = data.listIterator();
        byte[] d = null;
        ByteOutputArray boa = generateGzipJSON(ui, si, ds, iter, true);

        return publishData(requestURL, boa.data);
    }

    /**
     * Upload method for publishing data to the Cerebral Cortex webservice
     *
     * @param request URL
     * @param id      of the datastream to publish
     */
    public boolean publishGzipJSONData(String request, Integer id) {
        DataSource ds = getDataSource(id);
        UserInfo userInfo = getUserInfo();
        StudyInfo studyInfo = getStudyInfo();

        SQLiteIterator sqli = new SQLiteIterator(statement, id, PUBLISH_BUFFER_SIZE);

        boolean success = false;
        ByteOutputArray boa;
        int count = 0;
        do {
            System.out.print("Iteration: " + count++ + " ... ");
            boa = generateGzipJSON(userInfo, studyInfo, ds, sqli, true);
            success = publishData(request, boa.data);
            System.out.println("Done");
            if (!success) {
                return success;
            }
        } while (boa.additionalData);
        return success;
    }



    /**
     * Upload method for publishing data to the Cerebral Cortex webservice
     *
     * @param request URL
     * @param data    Byte[] of data to send to Cerebral Cortex
     */
    private boolean publishData(String request, byte[] data) {
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
        HttpResponse response;
        try {
            response = client.execute(post);
            System.out.println(response);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

}
