package net.sumitsu.qpidamqpjmstest.hornetqamqp;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestQueueConsumerSimplified {
	
	private static final String BROKER_URL = "amqp://localhost:10005";
	private static final String QUEUE_PROPNAME = "TestQueue";
	private static final String QUEUE_NAME = "jms.queue.TestQueue20131007";
	private static final String CNXNFACTORY_NAME = "TestConnectionFactory";
	
	private static final String AMQPJMS_JNDIPROPPREFIX_CONNECTIONFACTORY = "connectionfactory." + CNXNFACTORY_NAME;
	private static final String AMQPJMS_JNDIPROPPREFIX_QUEUE = "queue." + QUEUE_PROPNAME;
	
	private final Logger log;
	
	private Properties buildMessagingProperties() throws IllegalArgumentException {
		final Properties brokerProps;
		
		brokerProps = new Properties();
		brokerProps.setProperty(Context.INITIAL_CONTEXT_FACTORY, ProgrammaticQpidAMQContextFactory.class.getName());
		brokerProps.setProperty(AMQPJMS_JNDIPROPPREFIX_CONNECTIONFACTORY, BROKER_URL);
		brokerProps.setProperty(AMQPJMS_JNDIPROPPREFIX_QUEUE, QUEUE_NAME);
		return brokerProps;
	}

	private Collection<AutoCloseable> buildConsumer() throws ConsumerAppException {
		String logMsg;
		final String logPrefix;
		final Properties cnxnProps;
		Context cnxnFactoryContext = null;
		Queue eventQueue;
		QueueConnectionFactory queueCnxnFactory;
		QueueConnection queueCnxn;
		QueueSession queueSession;
		QueueReceiver queueConsumer;
		LinkedList<AutoCloseable> resources;
		
		logPrefix = "[buildConsumer] ";
		log.info(">>> " + logPrefix);
		
		cnxnProps = buildMessagingProperties();
		
		log.debug(logPrefix + "broker AMQP connection props: " + cnxnProps);
		try {
			try {
				cnxnFactoryContext = new InitialContext(cnxnProps);
			} catch (NamingException nExc) {
				logMsg = "Unable to establish Context for properties: " + cnxnProps + "; " + nExc.toString();
				log.error(logPrefix + logMsg, nExc);
				throw new ConsumerAppException(logMsg, nExc);
			}
			
			log.debug(logPrefix + "looking up connection factory: " + CNXNFACTORY_NAME + "...");
			try {
				queueCnxnFactory = (QueueConnectionFactory) cnxnFactoryContext.lookup(CNXNFACTORY_NAME);
				log.debug("Obtained connection factory: " + queueCnxnFactory + " (" + queueCnxnFactory.getClass().getName() + ")");
			} catch (NullPointerException npExc) {
				logMsg = "Unable to obtain connection factory: " + CNXNFACTORY_NAME + "; " + npExc.toString();
				log.error(logPrefix + logMsg, npExc);
				throw new ConsumerAppException(logMsg, npExc);
			} catch (ClassCastException ccExc) {
				logMsg = "Unable to obtain connection factory: " + CNXNFACTORY_NAME + "; " + ccExc.toString();
				log.error(logPrefix + logMsg, ccExc);
				throw new ConsumerAppException(logMsg, ccExc);
			} catch (NamingException nExc) {
				logMsg = "Unable to obtain connection factory: " + CNXNFACTORY_NAME + "; " + nExc.toString();
				log.error(logPrefix + logMsg, nExc);
				throw new ConsumerAppException(logMsg, nExc);
			}
			
			log.debug(logPrefix + "looking up queue " + QUEUE_PROPNAME + "...");
			try {
				eventQueue = (Queue) cnxnFactoryContext.lookup(QUEUE_PROPNAME);
			} catch (NullPointerException npExc) {
				logMsg = "Unable to obtain queue: " + QUEUE_PROPNAME + "; " + npExc.toString();
				log.error(logPrefix + logMsg, npExc);
				throw new ConsumerAppException(logMsg, npExc);
			} catch (ClassCastException ccExc) {
				logMsg = "Unable to obtain queue: " + QUEUE_PROPNAME + "; " + ccExc.toString();
				log.error(logPrefix + logMsg, ccExc);
				throw new ConsumerAppException(logMsg, ccExc);
			} catch (NamingException nExc) {
				logMsg = "Unable to obtain queue: " + QUEUE_PROPNAME + "; " + nExc.toString();
				log.error(logPrefix + logMsg, nExc);
				throw new ConsumerAppException(logMsg, nExc);
			}
			try {
				log.debug(logPrefix + "creating queue connection...");
				queueCnxn = queueCnxnFactory.createQueueConnection();
			} catch (JMSException jmsExc) {
				logMsg = "Failed to create queue connection; " + jmsExc.toString();
				log.error(logPrefix + logMsg, jmsExc);
				throw new ConsumerAppException(logMsg, jmsExc);
			}
			try {
				log.debug(logPrefix + "creating queue session...");
				queueSession = queueCnxn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
			} catch (JMSException jmsExc) {
				logMsg = "Failed to create queue session; " + jmsExc.toString();
				log.error(logPrefix + logMsg, jmsExc);
				throw new ConsumerAppException(logMsg, jmsExc);
			}
			try {
				log.debug(logPrefix + "creating queue consumer...");
				queueConsumer = queueSession.createReceiver(eventQueue);
			} catch (JMSException jmsExc) {
				logMsg = "Failed to build queue consumer; " + jmsExc.toString();
				log.error(logPrefix + logMsg, jmsExc);
				throw new ConsumerAppException(logMsg, jmsExc);
			}
			try {
				log.debug(logPrefix + "creating queue listener...");
				queueConsumer.setMessageListener(new TestConsumerListener());
			} catch (JMSException jmsExc) {
				logMsg = "Failed to queue session; " + jmsExc.toString();
				log.error(logPrefix + logMsg, jmsExc);
				throw new ConsumerAppException(logMsg, jmsExc);
			}
			try {
				queueCnxn.start();
			} catch (JMSException jmsExc) {
				logMsg = "Failed to queue session; " + jmsExc.toString();
				log.error(logPrefix + logMsg, jmsExc);
				throw new ConsumerAppException(logMsg, jmsExc);
			}
		} catch (ConsumerAppException lsExc) {
			throw lsExc;
		} finally {
			if (cnxnFactoryContext != null) {
				try {
					cnxnFactoryContext.close();
				} catch (NamingException nExc) {
					logMsg = "Failed to close JNDI context; " + nExc.toString();
					log.error(logPrefix + logMsg, nExc);
				}
			}
		}
		
		resources = new LinkedList<AutoCloseable>();
		resources.add(queueCnxn);
		
		log.info("<<< " + logPrefix);
		return Collections.unmodifiableList(resources);
	}
	
	public TestQueueConsumerSimplified() {
		this.log = LogManager.getLogger(getClass());
	}
	
	public static void main(String[] arguments) {
		TestQueueConsumerSimplified tqcs;
		
		tqcs = new TestQueueConsumerSimplified();
		try {
			tqcs.buildConsumer();
		} catch (Exception exc) {
			exc.printStackTrace();
		}
	}
}
