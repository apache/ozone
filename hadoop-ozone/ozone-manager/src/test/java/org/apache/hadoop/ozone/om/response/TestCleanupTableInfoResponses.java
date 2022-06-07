package org.apache.hadoop.ozone.om.response;

import com.google.common.collect.Maps;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.SaveSpaceUsageToFile;
import org.apache.hadoop.hdds.scm.storage.RatisBlockOutputStream;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.response.s3.security.S3GetSecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.om.response.TestCleanupTableInfo.OM_RESPONSE_PACKAGE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class TestCleanupTableInfoResponses{

  private static final Logger LOG = LoggerFactory.getLogger(
      RatisBlockOutputStream.class);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  /**
   * Creates a spy object over an instantiated OMMetadataManager, giving the
   * possibility to redefine behaviour. In the current implementation
   * there isn't any behaviour which is redefined.
   *
   * @return the OMMetadataManager spy instance created.
   * @throws IOException if I/O error occurs in setting up data store for the
   *                     metadata manager.
   */
  private OMMetadataManager createOMMetadataManagerSpy(File folder) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder;
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    return spy(new OmMetadataManagerImpl(conf));
  }

  private void mockTables(OMMetadataManager omMetadataManager,
                          Set<String> updatedTables,
                          boolean isFSO)
      throws IOException, IllegalAccessException {
    Map<String, Table> tableMap = omMetadataManager.listTables();
    Map<Table, Table> mockedTableMap = new HashMap<>();
    List<String> tables = new ArrayList<>(tableMap.keySet());
    for (String table:tables) {
      Table mockedTable = Mockito.spy(tableMap.get(table));
      Answer answer = invocationOnMock -> {
        String t = table;
        if (isFSO) {
          if (table.equals(OmMetadataManagerImpl.OPEN_KEY_TABLE)) {
            t = OmMetadataManagerImpl.OPEN_FILE_TABLE;
          }
          if (table.equals(OmMetadataManagerImpl.KEY_TABLE)) {
            t = OmMetadataManagerImpl.FILE_TABLE;
          }
        } else {
          if (table.equals(OmMetadataManagerImpl.OPEN_FILE_TABLE)) {
            t = OmMetadataManagerImpl.OPEN_KEY_TABLE;
          }
          if (table.equals(OmMetadataManagerImpl.FILE_TABLE)) {
            t = OmMetadataManagerImpl.KEY_TABLE;
          }
        }
        updatedTables.add(t);
        return null;
      };
      Mockito.lenient().doAnswer(answer).when(mockedTable).put(any(), any());
      Mockito.lenient().doAnswer(answer).when(mockedTable)
          .putWithBatch(any(), any(), any());
      Mockito.lenient().doAnswer(answer).when(mockedTable).delete(any());
      Mockito.lenient().doAnswer(answer).when(mockedTable).deleteWithBatch(any(), any());
      mockedTableMap.put(tableMap.get(table), mockedTable);
      tableMap.put(table, mockedTable);
    }
    for (Field f:omMetadataManager.getClass().getDeclaredFields()) {
      boolean isAccessible = f.isAccessible();
      f.setAccessible(true);
      try {
        if (Table.class.isAssignableFrom(f.getType())) {
          f.set(omMetadataManager,
              mockedTableMap.getOrDefault(f.get(omMetadataManager),
                  (Table)f.get(omMetadataManager)));
        }
      } finally {
        f.setAccessible(isAccessible);
      }
    }
  }
  private <T> T getMockObject(Class<T> type,
                              Map<Class, Object> instanceMap,
                              OzoneManagerProtocolProtos.Status status,
                              boolean isFSO) {
    T instance =  Mockito.mock(type, invocationOnMock -> {
      try {
        return createInstance(invocationOnMock.getMethod().getReturnType(),
            instanceMap, status, isFSO);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    });
    instanceMap.put(type, instance);
    return instance;
  }
  private List<Field> getFields(Class<?> type) {
    List<Field> fields = new ArrayList<>();
    while (type != Object.class) {
      fields.addAll(Arrays.asList(type.getDeclaredFields()));
      type = type.getSuperclass();
    }
    return fields;
  }
  private Object createInstance(Class<?> type,
                                Map<Class, Object> instanceMap,
                                OzoneManagerProtocolProtos.Status status, boolean isFSO) {
    if (instanceMap.containsKey(type)) {
      return instanceMap.get(type);
    }
    Object instance = null;
    if (type.isArray()) {
      return Array.newInstance(type.getComponentType(), 0);
    } else if (type.equals(Void.TYPE)) {
      return null;
    } else if (type.isPrimitive()) {
      if (Boolean.TYPE.equals(type)) {
        return true;
      } else if (Character.TYPE.equals(type)) {
        return 'a';
      } else if (Byte.TYPE.equals(type)) {
        return 0xFF;
      } else if (Short.TYPE.equals(type)) {
        return (short) 0;
      } else if (Integer.TYPE.equals(type)) {
        return 0;
      } else if (Long.TYPE.equals(type)) {
        return (long) 0;
      } else if (Float.TYPE.equals(type)) {
        return (float) 0.0;
      } else if (Double.TYPE.equals(type)) {
        return 0.0;
      } else if (Void.TYPE.equals(type)) {
        return null;
      }
    } else if (type.equals(OzoneManagerProtocolProtos.OMResponse.class)) {
      return OzoneManagerProtocolProtos.OMResponse.newBuilder()
          .setStatus(status).buildPartial();
    } else if (type.equals(BucketLayout.class)) {
      return isFSO ? BucketLayout.FILE_SYSTEM_OPTIMIZED
          : BucketLayout.DEFAULT;
    }
    try {
      if (type.isEnum()) {
        return type.getDeclaredFields()[0].get(type);
      } else if (type.equals(String.class)) {
        return "testString";
      } else if (Map.class.isAssignableFrom(type)) {
        return type.isInterface() ? Collections.EMPTY_MAP : type.newInstance();
      } else if (List.class.isAssignableFrom(type)) {
        return type.isInterface() ? Collections.EMPTY_LIST : type.newInstance();
      } else if (Set.class.isAssignableFrom(type)) {
        return type.isInterface() ? Collections.EMPTY_SET : type.newInstance();
      }
    } catch (IllegalAccessException e) {
      LOG.error("Could not access constructor for type {}",type.getName());
      return null;
    } catch (InstantiationException e) {
      LOG.error("Could not instantiate instance of type {}",type.getName());
      return null;
    }
    if (OMClientResponse.class.isAssignableFrom(type)) {
      Constructor<?>[] constructors = type.getDeclaredConstructors();
      for (Constructor c:constructors) {
        boolean accessible  = c.isAccessible();
        c.setAccessible(true);
        try {
          Class<?>[] params = c.getParameterTypes();
          boolean flag = false;
          for (Class<?> p:params) {
            if (p.equals(type)) {
              flag = true;
              break;
            }
          }
          if (flag) {
            continue;
          }
          instance = c.newInstance(Arrays.stream(params).map(p -> {
            return createInstance(p, instanceMap, status, isFSO);
          }).toArray());
          break;
        } catch (Exception e) {
          LOG.error("Error {} while creating instance using Constructor {} for type {} trying another constructor",e.getMessage(),c.getName(),type.getName());
        } finally {
          c.setAccessible(accessible);
        }
      }
      for (Field f:getFields(type)) {
        boolean isAccessible = f.isAccessible();
        f.setAccessible(true);
        try {
          Object fInstance = createInstance(f.getType(), instanceMap,
              status, isFSO);
          f.set(instance, fInstance);
        } catch (Exception e) {
          LOG.error("Error {} while setting field {} of type {}",e.getMessage(),f.getName(),f.getType().getName());
        } finally {
          f.setAccessible(isAccessible);
        }
      }
    } else {
      instance = getMockObject(type, instanceMap, status, isFSO);
    }
    return instance;
  }
  private OzoneManagerProtocolProtos.Status getExpectedStatusForResponseClass(
      Class<? extends OMClientResponse> responseClass) {
    Map<Class<? extends OMClientResponse>, OzoneManagerProtocolProtos.Status> responseClassStatusMap
        = new HashMap<>();
    responseClassStatusMap.put(S3GetSecretResponse.class, OzoneManagerProtocolProtos.Status.OK);
    return responseClassStatusMap.getOrDefault(responseClass, OzoneManagerProtocolProtos.Status.OK);
  }

  public Set<Class<? extends OMClientResponse>> responseClasses() {
    Reflections reflections = new Reflections(OM_RESPONSE_PACKAGE);
    return reflections.getSubTypesOf(OMClientResponse.class);
  }

  @Test
  public void checkCleanupTablesWithTableNames()
      throws IOException, IllegalAccessException {
    checkCleanupTablesWithTableNames(true);
    checkCleanupTablesWithTableNames(false);
  }
  private void checkCleanupTablesWithTableNames(boolean isFSO)
      throws IOException, IllegalAccessException {
    OMMetadataManager omMetadataManager = createOMMetadataManagerSpy(folder.newFolder());
    BatchOperation batchOperation = omMetadataManager.getStore().initBatchOperation();
    Set<Class<? extends OMClientResponse>> subTypes = responseClasses();
    subTypes = subTypes.stream()
        .filter(c -> c.isAnnotationPresent(CleanupTableInfo.class))
        .collect(Collectors.toSet());
    Set<String> updatedTables = new HashSet<>();
    mockTables(omMetadataManager, updatedTables, isFSO);

    Map<Class, Object> instanceMap = Maps.newHashMap();
    for (Class<? extends OMClientResponse> subType : subTypes) {
      if (subType.isAnnotationPresent(CleanupTableInfo.class) &&
          !Modifier.isAbstract(subType.getModifiers()) &&
          Arrays.stream(subType.getDeclaredMethods())
              .anyMatch(m -> m.getName().equals("addToDBBatch"))) {
        try {
          updatedTables.clear();
          instanceMap.clear();
          CleanupTableInfo cleanupTableInfo =
              subType.getAnnotation(CleanupTableInfo.class);
          OMClientResponse omClientResponse =
              (OMClientResponse) createInstance(subType,
                  instanceMap, getExpectedStatusForResponseClass(subType), isFSO);
          omClientResponse.addToDBBatch(omMetadataManager,
              batchOperation);
          Set<String> tables = Arrays.stream(cleanupTableInfo.cleanupTables())
              .collect(Collectors.toSet());
          if (isFSO) {
            if (tables.contains(OmMetadataManagerImpl.KEY_TABLE)) {
              tables.remove(OmMetadataManagerImpl.KEY_TABLE);
              tables.add(OmMetadataManagerImpl.FILE_TABLE);
            }
            if (tables.contains(OmMetadataManagerImpl.OPEN_KEY_TABLE)) {
              tables.remove(OmMetadataManagerImpl.OPEN_KEY_TABLE);
              tables.add(OmMetadataManagerImpl.OPEN_FILE_TABLE);
            }
          } else {
            if (tables.contains(OmMetadataManagerImpl.FILE_TABLE)) {
              tables.remove(OmMetadataManagerImpl.FILE_TABLE);
              tables.add(OmMetadataManagerImpl.KEY_TABLE);
            }
            if (tables.contains(OmMetadataManagerImpl.OPEN_FILE_TABLE)) {
              tables.remove(OmMetadataManagerImpl.OPEN_FILE_TABLE);
              tables.add(OmMetadataManagerImpl.OPEN_KEY_TABLE);
            }
          }
          for (String t:updatedTables) {
            Assert.assertTrue(
                String.format("Response Class: {} Tables for Cleanup: {},"
                        + " Tables Actually Updated: {}",
                    subType, tables, updatedTables), tables.contains(t));
          }
        } catch (Exception e) {
          LOG.error("Error while testing for Response Class {}", subType.getName());
          e.printStackTrace();
        }
      }
    }
  }
}
