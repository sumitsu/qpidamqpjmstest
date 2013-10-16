package net.sumitsu.qpidamqpjmstest.hornetqamqp;

import java.net.MalformedURLException;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.Context;
import javax.naming.NamingException;

import org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory;

public class ProgrammaticQpidAMQContextFactory extends PropertiesFileInitialContextFactory {

	@Override
	public Context getInitialContext(Hashtable environment) throws NamingException {
        Map data = new ConcurrentHashMap();

        try
        {
            createConnectionFactories(data, environment);
        }
        catch (MalformedURLException e)
        {
            NamingException ne = new NamingException();
            ne.setRootCause(e);
            throw ne;
        }

        createDestinations(data, environment);

        createQueues(data, environment);

        createTopics(data, environment);

        return createContext(data, environment);
    }

	public ProgrammaticQpidAMQContextFactory() {
		super();
	}

}
