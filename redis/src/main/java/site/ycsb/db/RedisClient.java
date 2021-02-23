/**
 * Copyright (c) 2021 YCSB contributors. All rights reserved.
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

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import redis.clients.jedis.*;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  private JedisCluster jedisCluster;
  private JedisPool jedisPool;
  private Jedis jedis;
  private Boolean clusterEnabled;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";

  public static final String INDEX_KEY = "_indices";

  public void init() throws DBException {
    Properties props = getProperties();
    int port = Protocol.DEFAULT_PORT;
    String host = Protocol.DEFAULT_HOST;
    int timeout = Protocol.DEFAULT_TIMEOUT;

    String redisTimeoutStr = props.getProperty(TIMEOUT_PROPERTY);
    String password = props.getProperty(PASSWORD_PROPERTY);
    clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    }
    if (props.getProperty(HOST_PROPERTY) != null){
      host = props.getProperty(HOST_PROPERTY);
    }
    if (redisTimeoutStr != null){
      timeout = Integer.parseInt(redisTimeoutStr);
    }

    if (clusterEnabled) {
      Set<HostAndPort> jedisClusterNodes = new HashSet<>();
      jedisClusterNodes.add(new HostAndPort(host, port));
      jedisCluster = new JedisCluster(jedisClusterNodes);
    } else {
      JedisPoolConfig poolConfig = new JedisPoolConfig();
      jedisPool = new JedisPool(poolConfig, host, port, timeout, password);
      jedis = jedisPool.getResource();
    }
  }

  public void cleanup() throws DBException {
    try {
      if (clusterEnabled) {
        ((Closeable) jedisCluster).close();
      } else {
        ((Closeable) jedis).close();
      }
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    if (fields == null) {
      Map<String, String> reply;
      if (clusterEnabled) {
        reply = jedisCluster.hgetAll(key);
      } else {
        reply = jedis.hgetAll(key);
      }
      extractHGetAllResults(result, reply);
    } else {
      List<String> reply;
      if (clusterEnabled) {
        reply = jedisCluster.hmget(key, fields.toArray(new String[fields.size()]));
      } else {
        reply = jedis.hmget(key, fields.toArray(new String[fields.size()]));
      }
      extractHmGetResults(fields, result, reply);
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
    assert !fieldIterator.hasNext() && !valueIterator.hasNext();
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    Jedis j;
    if (clusterEnabled) {
      j = jedisCluster.getConnectionFromSlot(Math.toIntExact(jedis.clusterKeySlot(key)));
    } else {
      j = jedis;
    }
    Pipeline p = j.pipelined();
    p.hmset(key, StringByteIterator.getStringMap(values));
    p.zadd(INDEX_KEY, hash(key), key);
    List<Object> res = p.syncAndReturnAll();
    final Status status = res.get(0).equals("OK") ? Status.OK : Status.ERROR;
    return status;
  }

  @Override
  public Status delete(String table, String key) {
    Jedis j;
    if (clusterEnabled) {
      j = jedisCluster.getConnectionFromSlot(Math.toIntExact(jedis.clusterKeySlot(key)));
    } else {
      j = jedis;
    }
    Pipeline p = j.pipelined();
    p.del(key);
    p.zrem(INDEX_KEY, key);
    List<Object> res = p.syncAndReturnAll();
    return res.get(0).equals(0) && res.get(1).equals(0) ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    String res;
    if (clusterEnabled) {
      res = jedisCluster.hmset(key, StringByteIterator.getStringMap(values));
    } else {
      res = jedis.hmset(key, StringByteIterator.getStringMap(values));
    }
    return res.equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Jedis j;
    if (clusterEnabled) {
      j = jedisCluster.getConnectionFromSlot(Math.toIntExact(jedis.clusterKeySlot(INDEX_KEY)));
    } else {
      j = jedis;
    }
    Set<String> keys = j.zrangeByScore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount);
    String[] fieldsArray = fields.toArray(new String[fields.size()]);
    Pipeline p = j.pipelined();
    if (fields == null) {
      for (String key : keys) {
        p.hgetAll(key);
      }
    } else {
      for (String key : keys) {
        p.hmget(key, fieldsArray);
      }
    }
    List<Object> res = p.syncAndReturnAll();
    if (fields == null) {
      for (Object reply: res) {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        extractHGetAllResults(values, (Map<String, String>) reply);
        result.add(values);
      }
    } else {
      for (Object reply: res) {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        extractHmGetResults(fields, values, (List<String>) reply);
        result.add(values);
      }
    }
    return Status.OK;
  }
}
