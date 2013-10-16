package net.sumitsu.qpidamqpjmstest.hornetqamqp;

import java.lang.reflect.Field;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionImpl;
import org.jgroups.util.UUID;

public class TestQueuePublisherSimplified {

	private static final String AMQPJMS_JNDIPROPPREFIX_CONNECTIONFACTORY = "connectionfactory.";
	private static final String AMQPJMS_JNDIPROPPREFIX_QUEUE = "queue.";
	
	private final Logger log;
	private QueueSession session;
	private final QueueSender producer;
	
	private boolean flipJMSMessageTypeTextObject;

	private void logValueForPrivateField(final Object cnxn, final String fieldName) throws NoSuchFieldException, IllegalAccessException {
		final Field privateField;
		final Object value;
		privateField = ConnectionImpl.class.getDeclaredField(fieldName);
		privateField.setAccessible(true);
		value = privateField.get(cnxn);
		log.debug(cnxn + "." + fieldName + "=" + value);
	}
	private void logConnectionImplInternalState(final QueueConnection cnxn) {
		try {
			logValueForPrivateField(cnxn, "_conn");
			logValueForPrivateField(cnxn, "_isQueueConnection");
			logValueForPrivateField(cnxn, "_isTopicConnection");
			logValueForPrivateField(cnxn, "_closeTasks");
			logValueForPrivateField(cnxn, "_host");
			logValueForPrivateField(cnxn, "_port");
			logValueForPrivateField(cnxn, "_username");
			logValueForPrivateField(cnxn, "_password");
			logValueForPrivateField(cnxn, "_remoteHost");
			logValueForPrivateField(cnxn, "_ssl");
			logValueForPrivateField(cnxn, "_clientId");
			logValueForPrivateField(cnxn, "_queuePrefix");
			logValueForPrivateField(cnxn, "_topicPrefix");
			logValueForPrivateField(cnxn, "_useBinaryMessageId");
		} catch (NoSuchFieldException nsfExc) {
			log.error(nsfExc);
		} catch (IllegalAccessException iaExc) {
			log.error(iaExc);
		}
	}
	
	private QueueSender buildProducer() throws Exception {
		final String logPrefix;
		final String dummySubscriptionId;
		final Properties brokerProps;
		final String cnxnFactoryLookupName;
		final String queueLookupName;
		Context cnxnFactoryContext = null;
		Queue eventQueue;
		QueueConnectionFactory queueCnxnFactory;
		QueueConnection queueCnxn;
		QueueSession queueSession;
		QueueSender queueProducer;
		
		logPrefix = "[buildProducer] ";
		
		dummySubscriptionId = UUID.randomUUID().toString();
		cnxnFactoryLookupName = "CnxnFactory-" + dummySubscriptionId;
		queueLookupName = "Queue-" + dummySubscriptionId;
		
		brokerProps = new Properties();
		brokerProps.setProperty(Context.INITIAL_CONTEXT_FACTORY, ProgrammaticQpidAMQContextFactory.class.getName());
		//brokerProps.setProperty(AMQPJMS_JNDIPROPPREFIX_CONNECTIONFACTORY + cnxnFactoryLookupName, "amqp://guest:guest@clientid/testvhost?brokerlist='tcp://localhost:10005'");
		//brokerProps.setProperty(AMQPJMS_JNDIPROPPREFIX_CONNECTIONFACTORY + cnxnFactoryLookupName, "amqp://guest:guest@localhost:10005/test?clientId='testclient'");
		brokerProps.setProperty(AMQPJMS_JNDIPROPPREFIX_CONNECTIONFACTORY + cnxnFactoryLookupName, "amqp://localhost:10005");
		brokerProps.setProperty(AMQPJMS_JNDIPROPPREFIX_QUEUE + queueLookupName, "jms.queue.TestQueue20131007");
		
		log.debug(logPrefix + "broker AMQP connection props: " + brokerProps);
		cnxnFactoryContext = new InitialContext(brokerProps);
		log.debug(logPrefix + "looking up connection factory: " + cnxnFactoryLookupName + "...");
		queueCnxnFactory = (QueueConnectionFactory) cnxnFactoryContext.lookup(cnxnFactoryLookupName);
		log.debug(logPrefix + "looking up queue: " + queueLookupName);
		eventQueue = (Queue) cnxnFactoryContext.lookup(queueLookupName);
		log.debug(logPrefix + "creating queue connection for subscription '" + dummySubscriptionId + "'...");
		queueCnxn = queueCnxnFactory.createQueueConnection();
		log.debug(logPrefix + "creating queue session for subscription '" + dummySubscriptionId + "'...");
		System.out.println(queueCnxn.getClass());
		logConnectionImplInternalState(queueCnxn);
		queueSession = queueCnxn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		this.session = queueSession;
		log.debug(logPrefix + "creating queue consumer for subscription '" + dummySubscriptionId + "'...");
		queueProducer = queueSession.createSender(eventQueue);
		
		cnxnFactoryContext.close();
		log.info("<<< " + logPrefix + "subscriptionId='" + dummySubscriptionId);
		return queueProducer;
	}
	
	public void sendMessage(String message) throws JMSException {
		TextMessage textMsg;
		ObjectMessage objMsg;
		
		if (this.flipJMSMessageTypeTextObject) {
			objMsg = session.createObjectMessage(message);
			log.debug("Sending text message: " + objMsg.getClass().toString());
			producer.send(objMsg);
		} else {
			textMsg = session.createTextMessage(message);
			log.debug("Sending text message: " + textMsg.getClass().toString());
			producer.send(textMsg);
		}
		this.flipJMSMessageTypeTextObject = !this.flipJMSMessageTypeTextObject;
	}
	
	public TestQueuePublisherSimplified() throws Exception {
		this.log = LogManager.getLogger(getClass());
		this.flipJMSMessageTypeTextObject = false;
		try {
			this.producer = buildProducer();
		} catch (Exception exc) {
			log.error(exc);
			throw exc;
		}
	}
	
	/*
	public static void main(String[] arguments) {
		TestQueuePublisherSimplified tqp = null;
		Scanner inputScanner;
		String input;
		StringBuilder strGen;
		
		try {
			tqp = new TestQueuePublisherSimplified();
			inputScanner = new Scanner(System.in);
			strGen = new StringBuilder();
			do {
				input = inputScanner.nextLine();
				if ("SEND".equals(input)) {
					System.out.println(">>> sending... >>>");
					tqp.sendMessage(strGen.toString());
					System.out.println(">>> sent! (" + strGen.length() + " characters) >>>");
					strGen.setLength(0);
				} else {
					if (strGen.length() > 0) {
						strGen.append("\n");
					}
					strGen.append(input);
				}
				if (input != null) {
					input = input.trim();
				}
			} while (!"QUIT".equals(input)); 
		} catch (Exception exc) {
			exc.printStackTrace();
		}
	}
	*/
	
	public static void main(String[] arguments) {
		TestQueuePublisherSimplified tqp = null;
		
		try {
			tqp = new TestQueuePublisherSimplified();
			while (true) {
				tqp.sendMessage("test message:" + System.currentTimeMillis() + ":" + UUID.randomUUID().toString());
				try {
					Thread.sleep(10000);
				} catch (InterruptedException iExc) {
					iExc.printStackTrace();
				}
			}
		} catch (Exception exc) {
			exc.printStackTrace();
		}
	}
}
