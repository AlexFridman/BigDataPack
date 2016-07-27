import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.*;
import twitter4j.conf.*;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
/**
 * 
 * @author dpanw
 *Kafka producer , gets streaming data from twitter source and feeds to consumer using topic
 *command   java -cp "kafka_2.11-0.10.0.0/libs/*":"twitter4j-4.0.4/lib/*":. KafkaTwitterProducer Pd0qRHkslklQ3yGSkAM9uagY
 *T 70sGcljH8V4Nq0i1ZpIVhpu619k2nqWm66XM7vAs2vo5A3Zhv3 2925168379-0TNkrhgcnzHM9r9OKdboxZwPVFXQC12nCM6XCAa 1XseTS2ZRrZaXJ6ViO4x7HZcez8YR4ckpDt65ul2jmXOR twitterproducer food
 *
 *twitterproducer is a name of topic and food is a keyword to relate tweets , command contains twitter app credentials
 *
 *compiling this producer :  javac -cp "kafka_2.11-0.10.0.0/libs/*":"twitter4j-4.0.4/lib/*":. KafkaTwitterProducer.java 
 */
public class KafkaTwitterProducer {
	public static void main(String[] args) throws Exception {
		LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

		if (args.length < 5) {
			System.out.println(
					"Usage: KafkaTwitterProducer <twitter-consumer-key> <twitter-consumer-secret> <twitter-access-token>"
							+ "  <twitter-access-token-secret><topic-name><twitter-search-keywords>");
			return;
		}

		String consumerKey = args[0].toString();
		String consumerSecret = args[1].toString();
		String accessToken = args[2].toString();
		String accessTokenSecret = args[3].toString();
		String topicName = args[4].toString();
		String[] arguments = args.clone();
		String[] keyWords = Arrays.copyOfRange(arguments, 5, arguments.length);

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

		StatusListener listner = new StatusListener() {

			public void onException(Exception arg0) {
				// TODO Auto-generated method stub

			}

			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub

			}

			public void onStatus(Status status) {
				queue.offer(status);
			}

			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub

			}

			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub

			}

			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub

			}
		};

		twitterStream.addListener(listner);
		for (String string : keyWords) {
			System.out.println("String " + string);
		}

		FilterQuery query = new FilterQuery().track(keyWords);
		System.out.println(query);

		twitterStream.filter(query);
		Thread.sleep(5000);

		// Add Kafka producer config settings
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("compression.codec", "2");

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		int i = 0;
		int j = 0;

		while (true) {
			Status ret = queue.poll();
			if (ret == null) {
				Thread.sleep(100);
				i++;
			} else {
				for (HashtagEntity hashtage : ret.getHashtagEntities()) {
					System.out.println("Hashtag: " + hashtage.getText());
					producer.send(
							new ProducerRecord<String, String>(topicName, Integer.toString(j++), hashtage.getText()));
				}
			}
		}
		// producer.close();
		// Thread.sleep(5000);
		// twitterStream.shutdown();
	}
}