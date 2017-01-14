package io.indexr.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * A fast json deserializer which parse json in utf-8 encoding bytes directly.
 * Using this class to avoid the pay-off of transforming into utf-16 (i.e. java String), and generating lots of intermediate objects.
 * It supports multiple json objects, seperated by comma, and each object can only contains one layer. e.g.
 * <pre>
 * {"name": "Flow", "score": 4.3}, {"title": "You know \"nothing\"! Jon Snow."}
 * </pre>
 */
public class UTF8JsonDeserializer {
    private static final Logger logger = LoggerFactory.getLogger(UTF8JsonDeserializer.class);

    private static final byte CURLY_BRACE_LEFT = '{';
    private static final byte CURLY_BRACE_RIGHT = '}';
    private static final byte DOUBLE_QUOTE = '"';
    private static final byte COLON = ':';
    private static final byte COMMA = ',';
    private static final byte BLANK = ' ';
    private static final byte BACKSLASH = '\\';

    private static final byte STATE_BEGIN = 0;
    private static final byte STATE_NEXT_KEY = 1;
    private static final byte STATE_IN_KEY = 2;
    private static final byte STATE_KEY_FINISH = 3;
    private static final byte STATE_NEXT_VALUE = 4;
    private static final byte STATE_IN_VALUE = 5;
    private static final byte STATE_VALUE_FINISH = 6;
    private static final byte STATE_FINISH = 7;

    // NOTE: Those should keep in sync with io.indexr.segment.ColumnType
    public static final byte INVALID = -1;
    public static final byte INT = 1;
    public static final byte LONG = 2;
    public static final byte FLOAT = 3;
    public static final byte DOUBLE = 4;
    public static final byte STRING = 5;

    private final boolean numberEmptyAsZero;

    private ByteBuffer valueBuffer;

    public UTF8JsonDeserializer(boolean numberEmptyAsZero) {
        this.numberEmptyAsZero = numberEmptyAsZero;
        this.valueBuffer = ByteBuffer.allocate(1 << 16);
    }

    public UTF8JsonDeserializer() {
        this(false);
    }

    public boolean parse(byte[] data, Listener listener) {
        return parse(data, 0, data.length, listener);
    }

    public boolean parse(byte[] data, int offset, int len, Listener listener) {
        byte state = STATE_BEGIN;
        boolean escape = false;
        boolean couldBeString = false;
        byte valueType = 0;
        valueBuffer.clear();
        int objectOffset = 0;

        int to = offset + len;
        int pos = offset;
        for (; pos < to; pos++) {
            byte b = data[pos];
            switch (state) {
                case STATE_BEGIN:
                    switch (b) {
                        case BLANK:
                            break;
                        case CURLY_BRACE_LEFT:
                            state = STATE_NEXT_KEY;
                            objectOffset = pos;
                            listener.onStartJson(objectOffset);
                            break;
                        default:
                            logErrorJson("expecting {", data, offset, len, pos);
                            return false;
                    }
                    break;
                case STATE_NEXT_KEY:
                    switch (b) {
                        case BLANK:
                            break;
                        case DOUBLE_QUOTE:
                            valueBuffer.clear();
                            state = STATE_IN_KEY;
                            break;
                        case CURLY_BRACE_RIGHT:
                            state = STATE_FINISH;
                            listener.onFinishJson(objectOffset, pos - objectOffset + 1);
                            break;
                        default:
                            logErrorJson("expecting \" or }", data, offset, len, pos);
                            return false;
                    }
                    break;
                case STATE_IN_KEY:
                    switch (b) {
                        case BACKSLASH:
                            if (escape) {
                                valueBuffer.put(b);
                                escape = false;
                            } else {
                                escape = true;
                            }
                            break;
                        case DOUBLE_QUOTE:
                            if (escape) {
                                valueBuffer.put(b);
                                escape = false;
                            } else {
                                valueBuffer.flip();
                                state = STATE_KEY_FINISH;
                                valueType = listener.onKey(valueBuffer, valueBuffer.limit());
                            }
                            break;
                        default:
                            if (escape) {
                                logErrorJson("illegal character after", data, offset, len, pos);
                                return false;
                            } else {
                                valueBuffer.put(b);
                            }
                            break;
                    }
                    break;
                case STATE_KEY_FINISH:
                    switch (b) {
                        case BLANK:
                            break;
                        case COLON:
                            state = STATE_NEXT_VALUE;
                            break;
                        default:
                            logErrorJson("expecting :", data, offset, len, pos);
                            return false;
                    }
                    break;
                case STATE_NEXT_VALUE:
                    switch (b) {
                        case BLANK:
                            break;
                        case DOUBLE_QUOTE:
                            valueBuffer.clear();
                            couldBeString = true;
                            // Enable {"number": "100"}
                            //if (isNumber(valueType)) {
                            //    logErrorJson("expecting number", data, offset, len, pos);
                            //    return false;
                            //}
                            state = STATE_IN_VALUE;
                            break;
                        case CURLY_BRACE_LEFT:
                            logErrorJson("multi-layer is not supported", data, offset, len, pos);
                            return false;
                        default:
                            valueBuffer.clear();
                            valueBuffer.put(b);
                            couldBeString = false;
                            state = STATE_IN_VALUE;
                            break;
                    }
                    break;
                case STATE_IN_VALUE:
                    switch (b) {
                        case BACKSLASH:
                            if (escape) {
                                valueBuffer.put(b);
                                escape = false;
                            } else {
                                escape = true;
                            }
                            break;
                        case DOUBLE_QUOTE:
                            if (escape) {
                                valueBuffer.put(b);
                                escape = false;
                            } else {
                                valueBuffer.flip();
                                state = STATE_VALUE_FINISH;
                                if (valueType == INVALID) {
                                    // Do nothing.
                                } else if (isNumber(valueType)) {
                                    if (!onNumber(valueType, valueBuffer, listener)) {
                                        logErrorJson("illegal number", data, offset, len, pos - valueBuffer.limit());
                                        return false;
                                    }
                                } else {
                                    if (!listener.onStringValue(valueBuffer, valueBuffer.remaining())) {
                                        logErrorJson("illegal string", data, offset, len, pos - valueBuffer.limit());
                                        return false;
                                    }
                                }
                            }
                            break;
                        case COMMA:
                            if (escape) {
                                logErrorJson("illegal character", data, offset, len, pos);
                                return false;
                            } else {
                                if (valueType == INVALID) {
                                    if (couldBeString) {
                                        valueBuffer.put(b);
                                    } else {
                                        state = STATE_NEXT_KEY;
                                    }
                                } else if (isNumber(valueType)) {
                                    valueBuffer.flip();
                                    state = STATE_NEXT_KEY;
                                    if (!onNumber(valueType, valueBuffer, listener)) {
                                        logErrorJson("not a number", data, offset, len, pos - valueBuffer.limit());
                                        return false;
                                    }
                                } else {
                                    valueBuffer.put(b);
                                }
                            }
                            break;
                        case CURLY_BRACE_RIGHT:
                            if (escape) {
                                logErrorJson("illegal character", data, offset, len, pos);
                                return false;
                            } else {
                                if (valueType == INVALID) {
                                    if (couldBeString) {
                                        valueBuffer.put(b);
                                    } else {
                                        state = STATE_FINISH;
                                        listener.onFinishJson(objectOffset, pos - objectOffset + 1);
                                    }
                                } else if (isNumber(valueType)) {
                                    valueBuffer.flip();
                                    state = STATE_FINISH;
                                    if (!onNumber(valueType, valueBuffer, listener)) {
                                        logErrorJson("not a number", data, offset, len, pos - valueBuffer.limit());
                                        return false;
                                    }
                                    listener.onFinishJson(objectOffset, pos - objectOffset + 1);
                                } else {
                                    valueBuffer.put(b);
                                }
                            }
                            break;
                        default:
                            if (escape) {
                                logErrorJson("illegal character", data, offset, len, pos);
                                return false;
                            } else {
                                valueBuffer.put(b);
                            }
                            break;
                    }
                    break;
                case STATE_VALUE_FINISH:
                    switch (b) {
                        case BLANK:
                            break;
                        case COMMA:
                            state = STATE_NEXT_KEY;
                            break;
                        case CURLY_BRACE_RIGHT:
                            state = STATE_FINISH;
                            listener.onFinishJson(objectOffset, pos - objectOffset + 1);
                            break;
                        default:
                            logErrorJson("expecting ,", data, offset, len, pos);
                            return false;
                    }
                    break;
                case STATE_FINISH:
                    switch (b) {
                        case BLANK:
                            break;
                        case COMMA:
                            state = STATE_BEGIN;
                            break;
                        default:
                            logErrorJson("expecting EOF", data, offset, len, pos);
                            return false;
                    }
                    break;
                default:
                    logErrorJson("illegal state: " + state, data, offset, len, pos);
                    return false;
            }
        }
        if (state != STATE_FINISH) {
            logErrorJson("incomplete json", data, offset, len, pos);
            return false;
        }
        return true;
    }

    private static boolean basicNumberCheck(ByteBuffer buffer) {
        byte firstByte = buffer.get(0);
        return (firstByte >= '0' && firstByte <= '9') || firstByte == '+' || firstByte == '-' || firstByte == '.';
    }

    private boolean onNumber(byte type, ByteBuffer buffer, Listener listener) {
        try {
            boolean isEmpty = buffer.remaining() == 0;
            if (isEmpty) {
                if (!numberEmptyAsZero) {
                    return false;
                }
                switch (type) {
                    case INT:
                        return listener.onIntValue(0);
                    case LONG:
                        return listener.onLongValue(0);
                    case FLOAT:
                        return listener.onFloatValue(0);
                    case DOUBLE:
                        return listener.onDoubleValue(0);
                    default:
                        throw new IllegalStateException("illegal type " + type);
                }
            }
            if (!basicNumberCheck(buffer)) {
                return false;
            }
            switch (type) {
                case INT:
                    return listener.onIntValue((int) UTF8Util.parseLong(buffer.array(), buffer.position(), buffer.remaining()));
                case LONG:
                    return listener.onLongValue(UTF8Util.parseLong(buffer.array(), buffer.position(), buffer.remaining()));
                case FLOAT:
                    return listener.onFloatValue(UTF8Util.parseFloat(buffer.array(), buffer.position(), buffer.remaining()));
                case DOUBLE:
                    return listener.onDoubleValue(UTF8Util.parseDouble(buffer.array(), buffer.position(), buffer.remaining()));
                default:
                    throw new IllegalStateException("illegal type " + type);
            }
        } catch (NumberFormatException e) {
            // HACK!!!
            return false;
            //try {
            //    switch (type) {
            //        case INT: {
            //            double v = UTF8Util.parseDouble(buffer.array(), buffer.position(), buffer.remaining());
            //            return listener.onIntValue((int) v);
            //        }
            //        case LONG: {
            //            double v = UTF8Util.parseDouble(buffer.array(), buffer.position(), buffer.remaining());
            //            return listener.onLongValue((long) v);
            //        }
            //        default:
            //            throw new IllegalStateException("illegal type " + type);
            //    }
            //} catch (NumberFormatException e2) {
            //    return false;
            //}
        }
    }

    private static void logErrorJson(String format, byte[] data, int offset, int len, int pos) {
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("{}, json: {}, error pos: {}",
                        format,
                        new String(data, offset, len, UTF8Util.UTF8_NAME),
                        new String(data, pos, (offset + len) - pos, UTF8Util.UTF8_NAME));
            }
        } catch (UnsupportedEncodingException e) {
            // Should not happen.
        }
    }

    public static interface Listener {

        void onStartJson(int offset);

        void onFinishJson(int offset, int len);

        /**
         * Found a key, return the value type.
         * Return <code>-1</code> means you don't care this key-value pair, which will not cause onXXValue fired.
         */
        byte onKey(ByteBuffer key, int size);

        /**
         * Found a string value, corresponding to last key.
         */
        boolean onStringValue(ByteBuffer value, int size);

        boolean onIntValue(int value);

        boolean onLongValue(long value);

        boolean onFloatValue(float value);

        boolean onDoubleValue(double value);
    }

    private static boolean isNumber(byte type) {
        return type == INT || type == LONG || type == FLOAT || type == DOUBLE;
    }
}
