package org.md2k.datakitapi.source.datasource;

import org.md2k.datakitapi.Constants;
import org.md2k.datakitapi.source.AbstractObject;
import org.md2k.datakitapi.source.application.Application;
import org.md2k.datakitapi.source.platform.Platform;
import org.md2k.datakitapi.source.platformapp.PlatformApp;

import java.io.*;

/*
 * Copyright (c) 2015, The University of Memphis, MD2K Center
 * - Syed Monowar Hossain <monowar.hossain@gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
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
public class DataSource extends AbstractObject implements Serializable {
    private static final long serialVersionUID = Constants.serialVersionUID;
    private DataSource[] lineage = null;
    private String dataType = null;
    private Platform platform = null;
    private PlatformApp platformApp = null;
    private Application application = null;
    private boolean persistent = true;

    DataSource(DataSourceBuilder dataSourceBuilder) {
        super(dataSourceBuilder);
        this.lineage = dataSourceBuilder.lineage;
        this.dataType = dataSourceBuilder.dataType;
        this.platform = dataSourceBuilder.platform;
        this.platformApp = dataSourceBuilder.platformApp;
        this.application = dataSourceBuilder.application;
        this.persistent = dataSourceBuilder.persistent;
    }
    public DataSourceBuilder toDataSourceBuilder(){
        DataSourceBuilder dataSourceBuilder = super.toDataSourceBuilder();
        dataSourceBuilder=dataSourceBuilder.setLineage(lineage).setDataType(dataType).
                setPlatform(platform).setPlatformApp(platformApp).setApplication(application).setPersistent(persistent);
        return dataSourceBuilder;
    }

    public DataSource[] getLineage() {
        return lineage;
    }

    public String getDataType() {
        return dataType;
    }

    public Platform getPlatform() {
        return platform;
    }

    public PlatformApp getPlatformApp() {
        return platformApp;
    }

    public Application getApplication() {
        return application;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public byte[] toBytes() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
            return bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return null;
    }

    public static DataSource fromBytes(byte[] dataSourceByteArray) {
        ByteArrayInputStream bis = new ByteArrayInputStream(dataSourceByteArray);
        DataSource dataSource = null;
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            dataSource = (DataSource) in.readObject();
            return dataSource;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return null;
    }
}

