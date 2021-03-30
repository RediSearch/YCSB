/**
 * Copyright (c) 2021 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 * <p>
 * RediSearch client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.
 * For scanning we use RediSearch's secondary index capabilities.
 */

package site.ycsb.db;

import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;
import site.ycsb.*;
import site.ycsb.workloads.CoreWorkload;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * YCSB binding for <a href="https://github.com/RediSearch/RediSearch/">RediSearch</a>.
 * <p>
 * See {@code redisearch/README.md} for details.
 */
public class RediSearchClient extends DB {
  public static final String HOST_PROPERTY = "redisearch.host";
  public static final String PORT_PROPERTY = "redisearch.port";
  public static final String PASSWORD_PROPERTY = "redisearch.password";
  public static final String CLUSTER_PROPERTY = "redisearch.cluster";
  public static final String TIMEOUT_PROPERTY = "redisearch.timeout";
  public static final String INDEX_NAME_PROPERTY = "redisearch.indexname";
  public static final String INDEX_NAME_PROPERTY_DEFAULT = "index";
  public static final String SCORE_FIELD_NAME_PROPERTY = "redisearch.scorefield";
  public static final String SCORE_FIELD_NAME_PROPERTY_DEFAULT = "__doc_hash__";
  private JedisCluster jedisCluster;
  private JedisPool jedisPool;
  private Boolean clusterEnabled;
  private int fieldCount;
  private String fieldPrefix;
  private String indexName;
  private String scoreField;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    int port = Protocol.DEFAULT_PORT;
    String host = Protocol.DEFAULT_HOST;
    int timeout = Protocol.DEFAULT_TIMEOUT;

    String redisTimeoutStr = props.getProperty(TIMEOUT_PROPERTY);
    String password = props.getProperty(PASSWORD_PROPERTY);
    clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    String portString = props.getProperty(PORT_PROPERTY);
    indexName = props.getProperty(INDEX_NAME_PROPERTY, INDEX_NAME_PROPERTY_DEFAULT);
    scoreField = props.getProperty(SCORE_FIELD_NAME_PROPERTY, SCORE_FIELD_NAME_PROPERTY_DEFAULT);
    if (portString != null) {
      port = Integer.parseInt(portString);
    }
    if (props.getProperty(HOST_PROPERTY) != null) {
      host = props.getProperty(HOST_PROPERTY);
    }
    if (redisTimeoutStr != null) {
      timeout = Integer.parseInt(redisTimeoutStr);
    }

    JedisPoolConfig poolConfig = new JedisPoolConfig();
    if (clusterEnabled) {
      Set<HostAndPort> startNodes = Collections.singleton(new HostAndPort(host, port));
      jedisCluster = new JedisCluster(startNodes, timeout, timeout, 5, password, poolConfig);
    } else {
      jedisPool = new JedisPool(poolConfig, host, port, timeout, password);
    }

    fieldCount = Integer.parseInt(props.getProperty(
        CoreWorkload.FIELD_COUNT_PROPERTY, CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
    fieldPrefix = props.getProperty(
        CoreWorkload.FIELD_NAME_PREFIX, CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
    List<String> indexCreateCmdArgs = indexCreateCmdArgs(indexName);

    try (Jedis setupPoolConn = getResource()) {
      setupPoolConn.sendCommand(RediSearchCommands.CREATE, indexCreateCmdArgs.toArray(String[]::new));
    } catch (redis.clients.jedis.exceptions.JedisDataException e) {
      if (!e.getMessage().contains("Index already exists")) {
        throw new DBException(e.getMessage());
      }
    }
  }

  private Jedis getResource() {
    if (clusterEnabled) {
      return jedisCluster.getConnectionFromSlot(ThreadLocalRandom.current()
          .nextInt(JedisCluster.HASHSLOTS));
    } else {
      return jedisPool.getResource();
    }
  }

  private Jedis getResource(String key) {
    if (clusterEnabled) {
      return jedisCluster.getConnectionFromSlot(JedisClusterCRC16.getCRC16(key));
    } else {
      return jedisPool.getResource();
    }
  }

  /**
   * Helper method to create the FT.CREATE command arguments, used to add a secondary index definition to Redis.
   *
   * @param iName Index name
   * @return
   */
  private List<String> indexCreateCmdArgs(String iName) {
    List<String> args = new ArrayList<>(Arrays.asList(iName, "ON", "HASH", "SCORE_FIELD", scoreField,
        "SCHEMA", scoreField, "NUMERIC"));
    return args;
  }

  @Override
  public void cleanup() throws DBException {
    try {
      if (clusterEnabled) {
        jedisCluster.close();
      } else {
        jedisPool.close();
      }
    } catch (Exception e) {
      throw new DBException("Closing connection failed.", e);
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode() / Double.MAX_VALUE;
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try (Jedis j = getResource(key)) {
      if (fields == null) {
        Map<String, String> reply = j.hgetAll(key);
        extractHGetAllResults(result, reply);
      } else {
        List<String> reply = j.hmget(key, fields.toArray(new String[fields.size()]));
        extractHmGetResults(fields, result, reply);
      }
    } catch (Exception e) {
      return Status.ERROR;
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  private void extractHGetAllResults(Map<String, ByteIterator> result, Map<String, String> reply) {
    StringByteIterator.putAllAsByteIterators(result, reply);
  }

  private void extractHmGetResults(Set<String> fields, Map<String, ByteIterator> result, List<String> values) {
    Iterator<String> fieldIterator = fields.iterator();
    Iterator<String> valueIterator = values.iterator();

    while (fieldIterator.hasNext() && valueIterator.hasNext()) {
      result.put(fieldIterator.next(),
          new StringByteIterator(valueIterator.next()));
    }
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    values.put(scoreField, new StringByteIterator(String.valueOf(hash(key))));
    try (Jedis j = getResource(key)) {
      j.hmset(key, StringByteIterator.getStringMap(values));
      return Status.OK;
    } catch (Exception e) {
      throw e;
//      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try (Jedis j = getResource(key)) {
      j.del(key);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    try (Jedis j = getResource(key)) {
      j.hmset(key, StringByteIterator.getStringMap(values));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  /**
   * As you will see below, there is the need to model a scan operation within different records,
   * in which we scan records in order, starting at a randomly chosen record key.
   * The number of records to scan is randomly chosen.
   * <p>
   * To model this within RedisSearch, we use FT.SEARCH and use computed hash score from the key name as the lower limit
   * for the search query, and set +inf as the upper limit of the search result.
   * The provided record count is passed via the LIMIT 0 <recordcound> FT.SEARCH argument.
   * <p>
   * Together, the above FT.SEARCH command arguments fully comply with a sorted, randomly chosen starting key, with
   * variadic record count replies.
   * <p>
   * Example FT.SEARCH command that a scan operation would generate.
   * "FT.SEARCH" "index" "*" \
   * "FILTER" __score__ "-6.17979116E8" +inf \
   * "LIMIT" "0" "54" \
   * "RETURN" "10" \
   * "field0" "field1" "field2" "field3" "field4" \
   * "field5" "field6" "field7" "field8" "field9"
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    List<Object> resp;
    try (Jedis j = getResource(startkey)) {
      resp = (List<Object>) j.sendCommand(RediSearchCommands.SEARCH,
          scanCommandArgs(indexName, recordcount, startkey, fields));
    } catch (Exception e) {
      return Status.ERROR;
    }
    long totalResult = (long) resp.get(0);
    for (int i = 1; i < resp.size(); i += 2) {
      String docname = new String((byte[]) resp.get(i));
      List<byte[]> docFields = (List<byte[]>) resp.get(i + 1);
      HashMap<String, ByteIterator> values = new HashMap<>();
      for (int k = 0; k < docFields.size(); k += 2) {
        values.put(SafeEncoder.encode(docFields.get(k)),
            new StringByteIterator(SafeEncoder.encode(docFields.get(k + 1))));
        result.add(values);
      }
    }
    return Status.OK;
  }

  /**
   * Helpher method to create the FT.SEARCH args used for the scan() operation.
   *
   * @param iName   RediSearch index name
   * @param rCount  return count
   * @param sKey    start key
   * @param rFields fields to retrieve
   * @return
   */
  private String[] scanCommandArgs(String iName, int rCount, String sKey, Set<String> rFields) {
    int returnFieldsCount = fieldCount;
    if (rFields != null) {
      returnFieldsCount = rFields.size();
    }
    List<String> scanSearchArgs = new ArrayList<>(Arrays.asList(iName, "*",
        "FILTER", scoreField, Double.toString(hash(sKey)), "+inf",
        "LIMIT", "0", String.valueOf(rCount),
        "RETURN", String.valueOf(returnFieldsCount)));

    if (rFields == null) {
      for (int i = 0; i < fieldCount; i++) {
        scanSearchArgs.add(String.format("%s%d", fieldPrefix, i));
      }
    } else {
      for (String field : rFields) {
        scanSearchArgs.add(field);
      }
    }
    return scanSearchArgs.toArray(String[]::new);
  }

  /**
   * RediSearch Protocol commands.
   */
  public enum RediSearchCommands implements ProtocolCommand {

    CREATE,
    SEARCH;

    private final byte[] raw;

    RediSearchCommands() {
      this.raw = SafeEncoder.encode("FT." + name());
    }

    @Override
    public byte[] getRaw() {
      return this.raw;
    }
  }
}
