package org.apache.hadoop.ozone.container.ozoneimpl;

import java.io.IOException;

public class BindException extends IOException {
    public BindException() {
    }

    public BindException(String message) {
        super(message);
    }

    public BindException(String message, Throwable cause) {
        super(message, cause);
    }

    public BindException(Throwable cause) {
        super(cause);
    }
}
