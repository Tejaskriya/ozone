/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import com.google.protobuf.Descriptors;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.OzoneConsts;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Get schema of value for scm.db, om.db or container db file.
 */
@CommandLine.Command(
    name = "schema",
    description = "Schema of value in metadataTable"
)
@MetaInfServices(SubcommandWithParent.class)
public class ValueSchema implements Callable<Void>, SubcommandWithParent {

  @CommandLine.ParentCommand
  private RDBParser parent;

  public static final Logger LOG = LoggerFactory.getLogger(ValueSchema.class);

  @CommandLine.Spec
  private static CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(names = {"--column_family", "--column-family", "--cf"},
      required = true,
      description = "Table name")
  private String tableName;

  @CommandLine.Option(names = {"--dnSchema", "--dn-schema", "-d"},
      description = "Datanode DB Schema Version: V1/V2/V3",
      defaultValue = "V3")
  private String dnDBSchemaVersion;

  private static volatile boolean exception;

  @Override
  public Void call() throws Exception {

    boolean success = true;

    List<ColumnFamilyDescriptor> cfDescList =
        RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());
    final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();

    try (ManagedRocksDB db = ManagedRocksDB.openReadOnly(
        parent.getDbPath(), cfDescList, cfHandleList)) {

      String dbPath = parent.getDbPath();
      Map<String, List<String>> fields = new HashMap<>();
      success = getValueFields(dbPath, fields);

      out().println(fields);
    }

    if (!success) {
      // Trick to set exit code to 1 on error.
      // TODO: Properly set exit code hopefully by refactoring GenericCli
      throw new Exception(
          "Exit code is non-zero. Check the error message above");
    }
    return null;
  }

  private boolean getValueFields(String dbPath, Map<String, List<String>> valueSchema) {

    dbPath = removeTrailingSlashIfNeeded(dbPath);
    DBDefinitionFactory.setDnDBSchemaVersion(dnDBSchemaVersion);
    DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(
        Paths.get(dbPath), new OzoneConfiguration());
    if (dbDefinition == null) {
      err().println("Error: Incorrect DB Path");
      return false;
    }
    final DBColumnFamilyDefinition<?, ?> columnFamilyDefinition =
        dbDefinition.getColumnFamily(tableName);
    if (columnFamilyDefinition == null) {
      err().print("Error: Table with name '" + tableName + "' not found");
      return false;
    }

    Class<?> c = columnFamilyDefinition.getValueType();
    List<Descriptors.FieldDescriptor> fieldDescriptors = getFieldsList(c);

    if (null == fieldDescriptors || fieldDescriptors.size() == 0) {
      return false;
    }

    for (Descriptors.FieldDescriptor field : fieldDescriptors) {

      if (field.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE)) {
        List<Descriptors.FieldDescriptor> subFields  = field.getMessageType().getFields();
        if (subFields.size() == 0) {
          valueSchema.put(field.getName(), new ArrayList<>());
        } else {
          List<String> subFieldsNameList = new ArrayList<>();
          for (Descriptors.FieldDescriptor subField: subFields) {
            subFieldsNameList.add(subField.getName());
          }
          valueSchema.put(field.getName(), subFieldsNameList);
        }
      } else {
        valueSchema.put(field.getName(), new ArrayList<>());
      }
    }

    return true;
  }

  private List<Descriptors.FieldDescriptor> getFieldsList(Class<?> c) {

    List<Descriptors.FieldDescriptor> fields = new ArrayList<>();

    if (c.isPrimitive() || c.equals(Long.class) || c.equals(String.class)) {
      err().println("Simple type: " + c.getName());
      return fields;
    } else {
      Class<?> returnType = null;
      Method[] methods = c.getMethods();
      for (Method method : methods) {
        if (method.getName().equals("getProtobuf") || method.getName().equals("getProto")) {
          returnType = method.getReturnType();
          break;
        }
      }
      if (null == returnType) {
        err().println("Error in getting value type");
        return fields;
      }
      try {
        Method newBuilderMethod = returnType.getMethod("newBuilder");
        Object builder = newBuilderMethod.invoke(returnType);

        Method getDescriptorForTypeMethod = newBuilderMethod.getReturnType().getMethod("getDescriptorForType");
        Object descriptor = getDescriptorForTypeMethod.invoke(builder);

        Method getFieldsMethod = getDescriptorForTypeMethod.getReturnType().getMethod("getFields");

        fields = (List<Descriptors.FieldDescriptor>) getFieldsMethod.invoke(descriptor);

      } catch (NoSuchMethodException e) {
        err().println("oops 1");
        //throw new RuntimeException(e);
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        err().println("oops 2");
        //throw new RuntimeException(e);
      }
    }
    return fields;
  }

  private static PrintWriter err() {
    return spec.commandLine().getErr();
  }

  private static PrintWriter out() {
    return spec.commandLine().getOut();
  }

  @Override
  public Class<?> getParentType() {
    return RDBParser.class;
  }

  private String removeTrailingSlashIfNeeded(String dbPath) {
    if (dbPath.endsWith(OzoneConsts.OZONE_URI_DELIMITER)) {
      dbPath = dbPath.substring(0, dbPath.length() - 1);
    }
    return dbPath;
  }
}
