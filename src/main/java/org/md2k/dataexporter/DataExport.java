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
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.md2k.cerebralcortex.CerebralCortexDataPackage;
import org.md2k.cerebralcortex.TSV;
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


public class DataExport {

    private Statement statement = null;

    public DataExport(String filename) {
        try {
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + filename);
            statement = connection.createStatement();
            statement.setQueryTimeout(30);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

    }


    private String generateJSON(DataSource ds, List<String> result) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        CerebralCortexDataPackage obj = new CerebralCortexDataPackage();

        obj.datasource = ds;

        for(String s: result) {
            obj.data.add(new TSV(s));
        }
        return gson.toJson(obj);
    }

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

    public byte[] generateGzipJSON(Integer id) {
        List<String> result = getTimeseriesDataStream(id);
        DataSource ds = getDataSource(id);

        String json = generateJSON(ds, result);
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

            Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename, false), "utf-8"));
            writer.write(json);
            writer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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

    public String getOutputFilename(Integer id) {
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

    public DataSource getDataSource(Integer id) {
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

    public List<String> getTimeseriesDataStream(Integer id) {
        List<String> result = new ArrayList<String>();
        try {
            ResultSet rs = statement.executeQuery("Select sample from data where datasource_id = " + id);
            while (rs.next()) {
                byte[] b = rs.getBytes("sample");

                DataType dt = DataType.fromBytes(b);

                if(dt instanceof DataTypeBoolean) {
                    String temp = Long.toString(dt.getDateTime());
                    temp += ", " + ((DataTypeBoolean) dt).getSample();
                    result.add(temp);
                }
                else if(dt instanceof DataTypeBooleanArray) {
                    String temp = Long.toString(dt.getDateTime());
                    for(Boolean d: ((DataTypeBooleanArray) dt).getSample()) {
                        temp += ", " + d;
                    }
                    result.add(temp);
                }
                
                else if(dt instanceof DataTypeByte) {
                    String temp = Long.toString(dt.getDateTime());
                    temp += ", " + ((DataTypeByte) dt).getSample();
                    result.add(temp);
                }
                else if(dt instanceof DataTypeByteArray) {
                    String temp = Long.toString(dt.getDateTime());
                    for(Byte d: ((DataTypeByteArray) dt).getSample()) {
                        temp += ", " + d;
                    }
                    result.add(temp);
                }
                
                else if(dt instanceof DataTypeDouble) {
                    String temp = Long.toString(dt.getDateTime());
                    temp += ", " + ((DataTypeDouble) dt).getSample();
                    result.add(temp);
                }
                else if(dt instanceof DataTypeDoubleArray) {
                    String temp = Long.toString(dt.getDateTime());
                    for(Double d: ((DataTypeDoubleArray) dt).getSample()) {
                        temp += ", " + d;
                    }
                    result.add(temp);
                }
                
                else if(dt instanceof DataTypeFloat) {
                    String temp = Long.toString(dt.getDateTime());
                    temp += ", " + ((DataTypeFloat) dt).getSample();
                    result.add(temp);
                }
                else if(dt instanceof DataTypeFloatArray) {
                    String temp = Long.toString(dt.getDateTime());
                    for(Float d: ((DataTypeFloatArray) dt).getSample()) {
                        temp += ", " + d;
                    }
                    result.add(temp);
                }
                
                else if(dt instanceof DataTypeInt) {
                    String temp = Long.toString(dt.getDateTime());
                    temp += ", " + ((DataTypeInt) dt).getSample();
                    result.add(temp);
                }
                else if(dt instanceof DataTypeIntArray) {
                    String temp = Long.toString(dt.getDateTime());
                    for(Integer d: ((DataTypeIntArray) dt).getSample()) {
                        temp += ", " + d;
                    }
                    result.add(temp);
                }
                
                else if(dt instanceof DataTypeLong) {
                    String temp = Long.toString(dt.getDateTime());
                    temp += ", " + ((DataTypeLong) dt).getSample();
                    result.add(temp);
                }
                else if(dt instanceof DataTypeLongArray) {
                    String temp = Long.toString(dt.getDateTime());
                    for(Long d: ((DataTypeLongArray) dt).getSample()) {
                        temp += ", " + d;
                    }
                    result.add(temp);
                }

                else if(dt instanceof DataTypeString) {
                    String temp = Long.toString(dt.getDateTime());
                    temp += ", " + ((DataTypeString) dt).getSample();
                    result.add(temp);
                }
                else if(dt instanceof DataTypeStringArray) {
                    String temp = Long.toString(dt.getDateTime());
                    for(String d: ((DataTypeStringArray) dt).getSample()) {
                        temp += ", " + d;
                    }
                    result.add(temp);
                }

                else {
                    System.out.println("Unknown Object");
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    private static String byteArray2Hex(final byte[] hash) {
        Formatter formatter = new Formatter();
        for (byte b : hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public void postData(String request, Integer id) {
        byte[] data = generateGzipJSON(id);
        String hash = null;
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            hash = byteArray2Hex(md.digest(data));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(request);

        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
        builder.addBinaryBody("file", data, ContentType.DEFAULT_BINARY, "3_BATTERY_359535058251051_PHONE.gz");
        builder.addTextBody("hash",hash,ContentType.DEFAULT_TEXT);

        HttpEntity entity = builder.build();
        post.setEntity(entity);
        HttpResponse response = null;
        try {
            response = client.execute(post);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(response);

    }
}
