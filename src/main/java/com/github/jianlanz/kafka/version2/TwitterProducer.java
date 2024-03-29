package com.github.jianlanz.kafka.version2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final String CONSUMER_KEY = "4i5WcPuot9wxMsLEDIHwDVFnm";
    private static final String CONSUMER_SECRET = "ut5nHxIflfvvmlekf0J62RygeHnljRI301gGdqdW1XtQUoXdXv";
    private static final String TOKEN = "916377164379049984-e18mbfQezqTR7XiX8hBUyeNS6N4EzLL";
    private static final String SECRET = "Ww2LFU3a6yWuTtM12VNwZ9KpbtPAX0T4hm5Mfy14eAUYT";
    private static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport");

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        // create a Twitter client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        // create a Kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Stopping application ...");
            LOGGER.info("Shutting down client from Twitter ...");
            client.stop();
            LOGGER.info("Closing producer ...");
            producer.close();
            LOGGER.info("Done");
        }));

        // loop to send tweets to Kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                LOGGER.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            LOGGER.error("Something had happened", e);
                        }
                    }
                });
            }
        }
        LOGGER.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        // create the producer
        return new KafkaProducer<>(properties);
    }
}
