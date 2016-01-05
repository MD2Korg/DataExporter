package org.md2k.datakitapi.source.datasource;

import org.md2k.datakitapi.Constants;
import org.md2k.datakitapi.source.AbstractObjectBuilder;
import org.md2k.datakitapi.source.application.Application;
import org.md2k.datakitapi.source.platform.Platform;
import org.md2k.datakitapi.source.platformapp.PlatformApp;

import java.io.Serializable;
import java.util.HashMap;

/*
 * Copyright (c) 2015, The University of Memphis, MD2K Center
 * - Syed Monowar Hossain <monowar.hossain@gmail.com>
 * All rights reserved.
 * <p/>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p/>
 * * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * <p/>
 * * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p/>
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
public class DataSourceBuilder extends AbstractObjectBuilder implements Serializable {
    private static final long serialVersionUID = Constants.serialVersionUID;
    public DataSource[] lineage = null;
    public String dataType = null;
    public Platform platform = null;
    public PlatformApp platformApp = null;
    public Application application = null;
    public boolean persistent = true;

    public DataSourceBuilder setType(String type) {
        super.setType(type);
        return this;
    }

    public DataSourceBuilder setId(String id) {
        super.setId(id);
        return this;
    }

    public DataSourceBuilder setDescription(String description) {
        super.setDescription(description);
        return this;
    }

    public DataSourceBuilder setMetadata(String key, String value) {
        super.setMetadata(key, value);
        return this;
    }

    public DataSourceBuilder setMetadata(HashMap<String, String> metadata) {
        super.setMetadata(metadata);
        return this;
    }

    public DataSourceBuilder setLineage(DataSource[] lineage) {
        this.lineage = lineage;
        return this;
    }

    public DataSourceBuilder setDataType(String dataType) {
        this.dataType = dataType;
        return this;
    }

    public DataSourceBuilder setPlatform(Platform platform) {
        this.platform = platform;
        return this;
    }

    public DataSourceBuilder setPlatformApp(PlatformApp platformApp) {
        this.platformApp = platformApp;
        return this;
    }

    public DataSourceBuilder setApplication(Application application) {
        this.application = application;
        return this;
    }

    public DataSourceBuilder setPersistent(boolean persistent) {
        this.persistent = persistent;
        return this;
    }

    public DataSource build() {
        return new DataSource(this);
    }

    public DataSourceBuilder() {
    }

    public DataSourceBuilder(DataSource dataSource) {
        this.metadata = dataSource.getMetadata();
        this.type = dataSource.getType();
        this.description = dataSource.getDescription();
        this.id = dataSource.getId();
        this.lineage = dataSource.getLineage();
        this.dataType = dataSource.getDataType();
        this.platform = dataSource.getPlatform();
        this.platformApp = dataSource.getPlatformApp();
        this.application = dataSource.getApplication();
        this.persistent = dataSource.isPersistent();
    }

}
