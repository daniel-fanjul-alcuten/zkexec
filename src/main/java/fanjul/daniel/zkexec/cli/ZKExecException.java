package fanjul.daniel.zkexec.cli;

public class ZKExecException extends RuntimeException {

    private static final long serialVersionUID = -3537804455495056280L;

    public ZKExecException() {
        super();
    }

    public ZKExecException(final String message) {
        super(message);
    }

    public ZKExecException(final Throwable cause) {
        super(cause);
    }

    public ZKExecException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
