package site.ycsb.workloads;

import com.github.javafaker.Faker;
import site.ycsb.*;
import site.ycsb.generator.*;

import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class CommerceWorkload extends CoreWorkload {

  /**
   * The name of the property for the of sentences in description paragraph of a product a record.
   */
  public static final String DESCRIPTION_SETENCE_COUNT_PROPERTY = "description-sentence-count";
  /**
   * Default number of sentences in description paragraph of a product a record.
   */
  public static final String DESCRIPTION_SETENCE_COUNT_PROPERTY_DEFAULT = "1";
  private Faker faker;
  private int descriptionSentenceCount;

  public static String buildKeyName(long keynum, int zeropadding, boolean orderedinserts) {
    if (!orderedinserts) {
      keynum = Utils.hash(keynum);
    }
    String value = Long.toString(keynum);
    int fill = zeropadding - value.length();
    String prekey = "product";
    for (int i = 0; i < fill; i++) {
      prekey += '0';
    }
    return prekey + value;
  }

  /**
   * Initialize the scenario.
   * Called once, in the main client thread, before any operations are started.
   */
  @Override
  public void init(Properties p) throws WorkloadException {
    faker = new Faker(new Locale("en"));
    descriptionSentenceCount =
        Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
    recordcount =
        Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
    if (recordcount == 0) {
      recordcount = Integer.MAX_VALUE;
    }
    String requestdistrib =
        p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);

    long insertstart =
        Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
    long insertcount =
        Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
    // Confirm valid values for insertstart and insertcount in relation to recordcount
    if (recordcount < (insertstart + insertcount)) {
      System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
      System.err.println("recordcount must be bigger than insertstart + insertcount.");
      System.exit(-1);
    }
    zeropadding =
        Integer.parseInt(p.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));

    orderedinserts = p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") != 0;

    keysequence = new CounterGenerator(insertstart);
    operationchooser = createOperationGenerator(p);

    transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount);
    if (requestdistrib.compareTo("uniform") == 0) {
      keychooser = new UniformLongGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("exponential") == 0) {
      double percentile = Double.parseDouble(p.getProperty(
          ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
      double frac = Double.parseDouble(p.getProperty(
          ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
          ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
      keychooser = new ExponentialGenerator(percentile, recordcount * frac);
    } else if (requestdistrib.compareTo("sequential") == 0) {
      keychooser = new SequentialGenerator(insertstart, insertstart + insertcount - 1);
    } else if (requestdistrib.compareTo("zipfian") == 0) {
      // it does this by generating a random "next key" in part by taking the modulus over the
      // number of keys.
      // If the number of keys changes, this would shift the modulus, and we don't want that to
      // change which keys are popular so we'll actually construct the scrambled zipfian generator
      // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
      // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
      // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
      // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
      // the keyspace doesn't change from the perspective of the scrambled zipfian generator
      final double insertproportion = Double.parseDouble(
          p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
      int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
      int expectednewkeys = (int) ((opcount) * insertproportion * 2.0); // 2 is fudge factor

      keychooser = new ScrambledZipfianGenerator(insertstart, insertstart + insertcount + expectednewkeys);
    } else if (requestdistrib.compareTo("latest") == 0) {
      keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
    } else if (requestdistrib.equals("hotspot")) {
      double hotsetfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
      double hotopnfraction =
          Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
      keychooser = new HotspotIntegerGenerator(insertstart, insertstart + insertcount - 1,
          hotsetfraction, hotopnfraction);
    } else {
      throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
    }

    fieldchooser = new UniformLongGenerator(0, fieldcount - 1);

    insertionRetryLimit = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
    insertionRetryInterval = Integer.parseInt(p.getProperty(
        INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));
  }

  /**
   * Do one insert operation. Because it will be called concurrently from multiple client threads, this
   * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
   * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
   * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
   * synchronized, since each thread has its own threadstate instance.
   *
   * @param db
   * @param threadstate
   */
  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    String dbkey = buildKeyName(keynum, zeropadding, orderedinserts);
    HashMap<String, ByteIterator> values = buildValues(dbkey);

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, dbkey, values);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return null != status && status.isOk();
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client threads, this
   * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
   * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
   * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
   * synchronized, since each thread has its own threadstate instance.
   *
   * @param db
   * @param threadstate
   * @return false if the workload knows it is done for this thread. Client will terminate the thread.
   * Return true otherwise. Return true for workloads that rely on operationcount. For workloads that read
   * traces from a file, return true when there are more to do, false when you are done.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    return false;
  }

  /**
   * Builds values for all fields.
   */
  protected HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<>();
    values.put("productName", new StringByteIterator(faker.commerce().productName()));
    values.put("productScore", new StringByteIterator(String.valueOf(1.0 - ThreadLocalRandom.current()
        .nextDouble())));
    values.put("code", new StringByteIterator(faker.code().ean13()));
    values.put("description", new StringByteIterator(faker.company().catchPhrase()));
    values.put("department", new StringByteIterator(faker.commerce().department()));
    // 0 for out-of-stock
    // 1 for in-stock
    values.put("inStock", new StringByteIterator(String.valueOf(ThreadLocalRandom.current()
        .nextInt(2))));
    // 0 for standardPrice
    // 1 for inSale
    values.put("inSale", new StringByteIterator(String.valueOf(ThreadLocalRandom.current()
        .nextInt(2))));
    values.put("color", new StringByteIterator(faker.commerce().color()));
    values.put("image", new StringByteIterator(faker.company().logo()));
    values.put("material", new StringByteIterator(faker.commerce().material()));
    values.put("price", new StringByteIterator(faker.commerce().price()));
    values.put("currencyCode", new StringByteIterator(faker.currency().code()));
    values.put("brand", new StringByteIterator(faker.company().name()));
    values.put("stockCount", new StringByteIterator(String.valueOf(ThreadLocalRandom.current()
        .nextInt(501))));
    values.put("creator", new StringByteIterator(faker.artist().name()));
    values.put("shipsFrom", new StringByteIterator(faker.country().countryCode3()));
    return values;
  }
}
