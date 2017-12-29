package cn.edu.tsinghua.iotdb.exception;


public class UnSupportedFillTypeException extends DeltaEngineRunningException{

    public UnSupportedFillTypeException(String message, Throwable cause) { super(message, cause);}

    public UnSupportedFillTypeException(String message) {
        super(message);
    }

    public UnSupportedFillTypeException(Throwable cause) {
        super(cause);
    }

}
