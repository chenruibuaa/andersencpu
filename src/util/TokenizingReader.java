package util;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;

public class TokenizingReader extends LineNumberReader {
  private final char[] buf;
  private int bufLen;
  private int lastChar;

  public TokenizingReader(Reader in, int maxTokenLength) {
    super(in, maxTokenLength * 2);
    buf = new char[maxTokenLength];
    resetInternal();
  }

  public TokenType readNextToken() throws IOException {
    TokenType retval = TokenType.EOF;
    boolean foundNeg = false;
    boolean hasLastChar = lastChar != -1;
    int r = lastChar;
    bufLen = 0;
    while (hasLastChar || (r = super.read()) != -1) {
      hasLastChar = false;
      char c = (char) r;
      buf[bufLen] = c;
      if (c == '\n') {
        if (bufLen == 0) {
          retval = TokenType.EOL;
          lastChar = -1;
        } else {
          lastChar = c;
        }
        break;
      } else if (Character.isWhitespace(c)) {
        if (bufLen == 0)
          continue;
        else
          break;
      } else if (Character.isDigit(c)) {
        if (bufLen == 0 || retval == TokenType.INTEGER) {
          bufLen++;
          retval = TokenType.INTEGER;
          lastChar = -1;
        } else {
          lastChar = c;
          break;
        }
      } else if (c == '-') {
        if (bufLen == 0) {
          bufLen++;
          retval = TokenType.INTEGER;
          lastChar = -1;
          foundNeg = true;
        } else {
          lastChar = c;
          break;
        }
      } else if (Character.isLetter(c)) {
        if (bufLen == 0 || retval == TokenType.STRING) {
          bufLen++;
          retval = TokenType.STRING;
          lastChar = -1;
        } else {
          lastChar = c;
          break;
        }
      } else {
        if (bufLen == 0) {
          bufLen++;
          retval = TokenType.UNKNOWN;
          lastChar = -1;
          break;
        } else {
          lastChar = c;
          break;
        }
      }
    }
    
    if (foundNeg && bufLen < 2) {
      retval = TokenType.UNKNOWN;
    }
    
    return retval;
  }
  
  public void readUntilEnd() throws IOException {
    TokenType t;
    while ((t = readNextToken()) != TokenType.EOF && t != TokenType.EOL) {
      ;
    }
  }

  @Override
  public long skip(long n) throws IOException {
    resetInternal();
    return super.skip(n);
  }
  
  @Override
  public void reset() throws IOException {
    resetInternal();
    super.reset();
  }
  
  @Override
  public int read() throws IOException {
    resetInternal();
    return super.read();
  }
  
  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    resetInternal();
    return super.read(cbuf, off, len);
  }
  
  private void resetInternal() {
    bufLen = 0;
    lastChar = -1;
  }
  
  public int getInteger() {
    boolean neg = false;
    int start = 0;
    if (bufLen > 0 && buf[0] == '-') {
      neg = true;
      start = 1;
    }
    int retval = 0;
    for (int i = start; i < bufLen; i++) {
      retval = retval * 10 + Character.digit(buf[i], 10);
    }
    if (neg)
      return -retval;
    return retval;
  }

  public int readInteger() throws IOException {
    TokenType t = readNextToken();
    assert t == TokenType.INTEGER;
    return getInteger();
  }

  public CharSequence getString() {
    return new MyCharSequence(0, bufLen);
  }

  public CharSequence readString() throws IOException {
    TokenType t = readNextToken();
    assert t == TokenType.STRING;
    return getString();
  }

  public enum TokenType {
    EOF, EOL, STRING, INTEGER, UNKNOWN;
  }

  private class MyCharSequence implements CharSequence {
    private final int start;
    private final int end;

    public MyCharSequence(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public char charAt(int arg0) {
      return buf[start + arg0];
    }

    @Override
    public int length() {
      return end - start;
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      return new MyCharSequence(this.start + start, this.end + end);
    }
    
    @Override
    public String toString() {
      return new String(buf, start, end - start);
    }
  }
}
