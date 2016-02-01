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

import org.md2k.datakitapi.datatype.DataType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class SQLiteIterator implements Iterator<List<String>> {
    private ResultSet rs;

    public SQLiteIterator(Statement statement, Integer id) {
        try {
            rs = statement.executeQuery("Select sample from data where datasource_id = " + id);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        boolean result = false;
        try {
            result = !rs.isAfterLast();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public List<String> next() {
        List<String> result = new ArrayList<String>();
        try {
            while (result.size() < 10000 && rs.next()) {
                byte[] b = rs.getBytes("sample");
                DataType dt = DataType.fromBytes(b);
                result.add(DataTypeConverter.DataTypeToString(dt));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }
}
