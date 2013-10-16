package net.sumitsu.qpidamqpjmstest.hornetqamqp;

public class ConsumerAppException extends Exception {

	private static final long serialVersionUID = 7477918178349949706L;
	
	public ConsumerAppException(String message) {
		super(message);
	}
	public ConsumerAppException(Throwable cause) {
		super(cause);
	}
	public ConsumerAppException(String message, Throwable cause) {
		super(message, cause);
	}
	public ConsumerAppException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
