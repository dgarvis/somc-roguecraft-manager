package dev.garvis.somcroguecraft;

import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Date;
import java.util.Properties;
import java.util.Arrays;

import java.time.Duration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;


import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.errors.TimeoutException;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Manages sending and reading messages from a Kafka service.
 * When the class is created, messages can be sent to the class, 
 * however they will not be sent to the kafka server until the
 * connected is called successfully.
 */
public class KafkaManager {

    /**
     * The structure of a message.
     */
    public class Message extends HashMap<String, Object> {

	public Message() {
	    super();
	}

	public Message(Map<String,Object> m) {
	    super(m);
	}
    }

    /**
     * Function to be called when a message is ready to be
     * processed.
     */
    @FunctionalInterface
    interface ConsumerCallback {
	// https://www.w3docs.com/snippets/java/how-to-pass-a-function-as-a-parameter-in-java.html
	void apply(LinkedList<Message> messages);
    }    

    /**
     * Holds messages that need to be sent to the kafka.
     * When using, wrap in a `synchronized (messageSendQueue) { }`
     *
     * Note: these messages need to have the `server` key set.
     */
    protected LinkedList<Message> messageSendQueue;

    /**
     * Max number of messages to keep in the queue before dropping messages
     */
    protected int maxMessageSendQueue;

    /**
     * The name of this producer / consumer group.
     */
    protected String name;

    /**
     * Topic to send events to.
     * Only requried if the producer is used.
     */
    private String sendTopic;

    /**
     * Events to listen for and consume.
     * Only requried if the consumer is used.
     */
    private List<String> consumeEvents;
    
    /**
     * Function to pass messages to.
     * Only requried if the consumer is used.
     */
    private ConsumerCallback consumeCallback;

    /** 
     * The kafka producer, if configured.
     */
    private KafkaProducer<String, String> producer;

    /**
    * The kafka consumer, if configured.
    */
    private KafkaConsumer<String, String> consumer;

    /**
     * Establish a base class that can recieve messages.
     */
    public KafkaManager() {
	this(100000);
    }

    /**
     * Establish a base class that can recieve message, and set
     * the max queue size.
     *
     * @param maxMessageQueueSize The max number of messages to allow
     *          in the queue.
     */
    public KafkaManager(int maxMessageQueueSize) {
	this.maxMessageSendQueue = maxMessageQueueSize;
	this.messageSendQueue = new LinkedList<>();
    }

    /**
     * Add a message to the queue.
     * 
     * @param msg The message to add to the send queue.
     * @return True if their was space in the queue.
     */
    protected boolean addMessageToSendQueue(Message msg) {
	synchronized (this.messageSendQueue) {
	    if (this.messageSendQueue.size() > this.maxMessageSendQueue)
		return false;
	    this.messageSendQueue.addLast(msg);
	}
	return true;
    }

    /**
     * Get the next message to send. If the message was
     * sent successfully you should then call sentSuccess.
     *
     * @param the max number of messages to return for sending.
     * @return A list of messages.
     */
    protected LinkedList<Message> getMessagesToSend(int max) {
	LinkedList<Message> re = new LinkedList<>();
	synchronized (this.messageSendQueue) {
	    for (int i = 0; i < max && i < this.messageSendQueue.size(); i++)
		re.add(this.messageSendQueue.get(i));
	}
	return re;
    }

    /**
     * Removes the message from the queue, so the next message
     * can be gotten to send
     * 
     * @param amount The amount of messages that were sent. You should
     *    not use the same value as max, but instead the size of the 
     *    number of messages that were actually returned.
     */
    protected void sentSuccess(int amount) {
	synchronized (this.messageSendQueue) {
	    for (int i = 0; i < amount; i++)
		this.messageSendQueue.removeFirst();
	}
    }

    /** 
     * Connect to kafka, in send message only mode.
     *
     * @param name Name of the prodocer.
     * @param brokers The kafka brokers to connect to.
     * @param sendTopic topic to send the events to.
     * @return True if connected, otherwise false.
     */
    public boolean connect(String name, String brokers, String sendTopic) throws Exception {
	return this.connect(name, brokers, sendTopic, null, null, null);
    }

    /**
     * Connect to kafka, in consume messages only mode.
     *
     * @param name Name of the consumer group.
     * @param brokers The kafka brokers to connect to.
     * @param consumeTopics A list of topics to listen on.
     * @param consumeEvents A list of events to consume. Checks for a field called `eventType`.
     * @param consumeCallback A function to call with a list of messages to consume.
     * @return True if connected, otherwise false.
     */
    public boolean connect(String name, String brokers,
			   String[] consumeTopics, String[] consumeEvents,
			   ConsumerCallback consumeCallback) throws Exception {
	return this.connect(name, brokers, null, consumeTopics, consumeEvents, consumeCallback);
    }


    /**
     * Connect to kafka, with both consumer and producer.
     *
     * @param name Name of the consumer group and of the producer.
     * @param brokers The kafka brokers to connect to.
     * @param sendTopic topic to send the events to.
     * @param consumeTopics A list of topics to listen on.
     * @param consumeEvents A list of events to consume. Checks for a field called `eventType`.
     * @param consumeCallback A function to call with a list of messages to consume.
     * @return True if connected, otherwise false.
     */
    public boolean connect(String name, String brokers, String sendTopic,
			   String[] consumeTopics, String[] consumeEvents,
			   ConsumerCallback consumeCallback) throws Exception {

	if (name.isEmpty())
	    throw new Exception("A name must be provided.");

	if (brokers.isEmpty())
	    throw new Exception("Broker(s) must be provided.");

	this.name = name;

	if (!sendTopic.isEmpty()) {
	    startProducer(brokers, sendTopic);
	}

	if (consumeTopics.length > 0) {
	    startConsumer(brokers, consumeTopics, consumeEvents, consumeCallback);
	}
	
	// This return is actually fake
	return true;
    }

    /**
     * Will create the producer and start the message sending thread.
     */
    private void startProducer(String brokers, String sendTopic) throws Exception {
	if (this.name == null || this.name.isEmpty())
	    throw new Exception("Name was not set on the class.");
	if (brokers == null || brokers.isEmpty())
	    throw new Exception("Brokers is required");
	if (sendTopic == null || sendTopic.isEmpty())
	    throw new Exception("Topic to send to is required.");

	this.sendTopic = sendTopic;

	// Create the producer
	Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
	props.put(ProducerConfig.CLIENT_ID_CONFIG, this.name);
	try {
	    props.put("key.serializer",
		      Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
	    props.put("value.serializer",
		      Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
	} catch(Exception e) {
	    System.err.println("Error when setting serializers: " + e.toString());
	    throw e; // keep throwing this.
	}
	
	// create the producer
	this.producer = new KafkaProducer<>(props);
	
	// Start the producer thread.
	startMessageSender();
    }

    /**
     * Setup and start a thread for consuming events.
     */
    private void startConsumer(String brokers, String[] topics,
			       String[] events,
			       ConsumerCallback callback) throws Exception {
	if (this.name == null || this.name.isEmpty())
	    throw new Exception("Name was not set on the class.");
	if (brokers == null || brokers.isEmpty())
	    throw new Exception("Brokers is required");
	if (topics == null | topics.length <= 0)
	    throw new Exception("Topic(s) to consume from are required.");
	if (events == null || events.length <= 0)
	    throw new Exception("Events to consumer are required.");
	if (callback == null)
	    throw new Exception("Callback for events is required.");

	this.consumeCallback = callback;
	this.consumeEvents = Arrays.asList(events);
	
	// Setup Consumer
	Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
	props.put("group.id", this.name);
	props.put("enable.auto.commit", "true");
	props.put("max.poll.records", 50);
	try {
	    props.put("key.deserializer",
		      Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));
	    props.put("value.deserializer",
		      Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));
	} catch(Exception e) {
	    System.err.println("Error when setting serializers: " + e.toString());
	    throw e; // keep throwing this.
	}
	
	this.consumer = new KafkaConsumer<>(props);

	// Subscribe to topics
	this.consumer.subscribe(List.of(topics));

	// Start consumer thread.
	this.startMessageConsumer();
    }

    /**
     * Addes a message to the queue to be sent to the kafka stream.
     * This function will add / overwrite any existing `stamp` key 
     * and give it a value of the epoch time this function was called at.
     * Indirectly, this function will also set the `server` key, but
     * not until right before the message is sent to the server.
     *
     * @param msg The message to send to kafka.
     * @return True if their was space for the message in the queue.
     */
    public boolean sendMessage(Message msg) {
	msg.put("stamp", new Date().getTime() / 1000L);
	return this.addMessageToSendQueue(msg);
    }

    /**
     * Directly sends a message to the kafka topic.
     * 
     * @param msg the raw message to send to the kafka topic.
     * @return True if sent, otherwise false.
     */
    protected boolean sendMessageRaw(String msg) {
	try {
	    this.producer.send(new ProducerRecord<String, String>(this.sendTopic, msg));
	} catch (TimeoutException e) {
	    System.err.println("Kafka Timeout");
	    return false;
	} catch (Exception e) {
	    System.err.println("Kafka error when sending message, Error: " + e.toString());
	    return false;
	}

	return true;
    }

    /**
     * Stop all sub threads and close connections. Object should 
     * not be used after calling this.
     */
    public void close() {
	this.producer = null;
	this.consumer = null;
	
	// TODO - wait for threads to stop.
    }

    /**
     * Starts a sub thread that will take messages from the message send 
     * queue, add the server field, convert them to json, and send them
     * to the topic. Wil keep running until producer is set to null.
     */
    private void startMessageSender() {
	new Thread(() -> {
		while (this.producer != null) {		    
		    final int max_messages = 500;
		    ObjectMapper mapper = new ObjectMapper();
		    LinkedList<Message> messages = this.getMessagesToSend(max_messages);
		    int sentCount = 0;
		    
		    for (Message message : messages) {
			// Add server name
			message.put("server", this.name);

			// Convert to json
			String json;
			try {
			    json = mapper.writeValueAsString(message);
			} catch (Exception e) {
			    System.err.println("Could not convert object to json for kakfa stream. Error: " + e.toString());
			    // we are going to just keep processing messages
			    // and let this one get marked as done as something
			    // is probbaly wrong with the object and it is better
			    // to try and still send the other messages instead of
			    // getting stuck here in a loop.
			    sentCount++;
			    continue;
			}

			// Send the message
			if (!this.sendMessageRaw(json)) {
			    // So the message failed to send... let's stop
			    // processing this batch as we are probably
			    // not connected to kafka.
			    break;
			}
			sentCount++;
		    }

		    // Mark the messages sent as done in our queue.
		    this.sentSuccess(sentCount);

		    // Sleep for a second if sent count was zero.
		    if (sentCount <= 0) {
			try {
			    Thread.sleep(1000);
			} catch (Exception e) {
			    // don't care.
			}
			      
		    }
		}
	}).start();
    }

    private void startMessageConsumer() {
	new Thread(() -> {
		ObjectMapper mapper = new ObjectMapper();
					
		while (this.consumer != null) {

		    // Get Messages
		    ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofSeconds(5));

		    LinkedList<Message> messages = new LinkedList<>();
		    
		    // Parse Messages
		    for (ConsumerRecord<String, String> record : records) {
			try {
			    Map<String, Object> _message = mapper.readValue(record.value(), Map.class);
			    Message message = new Message(_message);
			    //Message message = mapper.readValue(record.value(), Message.class);
			    
			    // We can only handle messages with eventTypes
			    if (!message.containsKey("eventType")) 
				continue;

			    // We don't want messages from ourself.
			    if (message.containsKey("server") &&
				((String)message.get("server")).equals(this.name))
				continue;

			    // We only want messages that we want to listen for.
			    if (!this.consumeEvents.contains((String)message.get("eventType")))
				continue;

			    // Add message to queue for call back.
			    messages.add(message);
				
			} catch (Exception e) {
			    System.err.println("Error parsing mesasge: " +
					       e.toString());
			    // We are going to ignore this message and continue.
			    continue;
			}
		    }

		    // Send messages to callback
		    if (messages.size() > 0)
			this.consumeCallback.apply(messages);
		}

	}).start();
    }
}
