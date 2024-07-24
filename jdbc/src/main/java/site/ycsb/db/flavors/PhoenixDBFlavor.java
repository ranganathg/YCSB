/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db.flavors;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.db.JdbcDBClient;
import site.ycsb.db.StatementType;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

/**
 * Database flavor for Apache Phoenix. Captures syntax differences used by Phoenix.
 */
public class PhoenixDBFlavor extends DefaultDBFlavor {
  private static boolean useJson = false;
  public static final String JSON_COLUMN_NAME = "JSON_COL";
  protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  public PhoenixDBFlavor(boolean useJson) {
    super(DBName.PHOENIX);
    this.useJson = useJson;
  }

  @Override
  public String createInsertStatement(StatementType insertType, String key) {
    // Phoenix uses UPSERT syntax
    StringBuilder insert = new StringBuilder("UPSERT INTO ");
    insert.append(insertType.getTableName());
    if (!useJson) {
      insert.append(" (" + JdbcDBClient.PRIMARY_KEY + "," + insertType.getFieldString() + ")");
      insert.append(" VALUES(?");
      for (int i = 0; i < insertType.getNumFields(); i++) {
        insert.append(",?");
      }
    } else {
      insert.append(" (" + JdbcDBClient.PRIMARY_KEY + "," + PhoenixDBFlavor.JSON_COLUMN_NAME + ")");
      insert.append(" VALUES(?,?");
    }
    insert.append(")");
    return insert.toString();
  }

  @Override
  public String createUpdateStatement(StatementType updateType, String key) {
    // Phoenix doesn't have UPDATE semantics, just re-use UPSERT VALUES on the specific columns
    String[] fieldKeys = updateType.getFieldString().split(",");
    StringBuilder update = new StringBuilder("UPSERT INTO ");
    update.append(updateType.getTableName());
    update.append(" (");
    if (!useJson) {
      // Each column to update
      for (int i = 0; i < fieldKeys.length; i++) {
        update.append(fieldKeys[i]).append(",");
      }
      // And then set the primary key column
      update.append(JdbcDBClient.PRIMARY_KEY).append(") VALUES(");
      // Add an unbound param for each column to update
      for (int i = 0; i < fieldKeys.length; i++) {
        update.append("?, ");
      }
      // Then the primary key column's value
      update.append("?)");
    } else {
      update.append(JdbcDBClient.PRIMARY_KEY + ",").append(PhoenixDBFlavor.JSON_COLUMN_NAME)
          .append(") VALUES(?,?)");
    }
    return update.toString();
  }

  public static String encode(final Map<String, ByteIterator> source) {
    Map<String, String> stringMap = StringByteIterator.getStringMap(source);
    ObjectNode node = JSON_MAPPER.createObjectNode();
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    try {
      JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer);
      JSON_MAPPER.writeTree(jsonGenerator, node);
    } catch (Exception e) {
      throw new RuntimeException("Could not encode JSON value");
    }
    return writer.toString();
  }
}
