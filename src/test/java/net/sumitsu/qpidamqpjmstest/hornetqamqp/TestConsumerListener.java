package net.sumitsu.qpidamqpjmstest.hornetqamqp;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestConsumerListener implements MessageListener {

	private final Logger log;
	
	@Override
	public void onMessage(Message jmsMsg) {
		final TextMessage txtMsg;
		final ObjectMessage objMsg;
		try {
			if (jmsMsg instanceof TextMessage) {
				txtMsg = (TextMessage) jmsMsg;
				log.info("Received TEXT message: " + txtMsg.getText());
			} else if (jmsMsg instanceof ObjectMessage) {
				objMsg = (ObjectMessage) jmsMsg;
				log.info("Received OBJECT message" + objMsg.getObject());
			} else {
				log.warn("Unrecognized message type: " + jmsMsg.getClass());
			}
		} catch (Exception exc) {
			log.error("Exception in onMessage: " + exc.toString(), exc);
		}
	}

	public TestConsumerListener() {
		this.log = LogManager.getLogger(getClass());
	}
}
