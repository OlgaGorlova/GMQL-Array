/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package it.polimi.genomics.avro.garray;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class gCoords extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 3913492643341723019L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"gCoords\",\"namespace\":\"it.polimi.genomics.avro.garray\",\"fields\":[{\"name\":\"chr\",\"type\":\"string\"},{\"name\":\"start\",\"type\":\"long\"},{\"name\":\"stop\",\"type\":\"long\"},{\"name\":\"strand\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<gCoords> ENCODER =
      new BinaryMessageEncoder<gCoords>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<gCoords> DECODER =
      new BinaryMessageDecoder<gCoords>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<gCoords> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<gCoords> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<gCoords> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<gCoords>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this gCoords to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a gCoords from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a gCoords instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static gCoords fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private java.lang.CharSequence chr;
   private long start;
   private long stop;
   private java.lang.CharSequence strand;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public gCoords() {}

  /**
   * All-args constructor.
   * @param chr The new value for chr
   * @param start The new value for start
   * @param stop The new value for stop
   * @param strand The new value for strand
   */
  public gCoords(java.lang.CharSequence chr, java.lang.Long start, java.lang.Long stop, java.lang.CharSequence strand) {
    this.chr = chr;
    this.start = start;
    this.stop = stop;
    this.strand = strand;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return chr;
    case 1: return start;
    case 2: return stop;
    case 3: return strand;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: chr = (java.lang.CharSequence)value$; break;
    case 1: start = (java.lang.Long)value$; break;
    case 2: stop = (java.lang.Long)value$; break;
    case 3: strand = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'chr' field.
   * @return The value of the 'chr' field.
   */
  public java.lang.CharSequence getChr() {
    return chr;
  }


  /**
   * Sets the value of the 'chr' field.
   * @param value the value to set.
   */
  public void setChr(java.lang.CharSequence value) {
    this.chr = value;
  }

  /**
   * Gets the value of the 'start' field.
   * @return The value of the 'start' field.
   */
  public long getStart() {
    return start;
  }


  /**
   * Sets the value of the 'start' field.
   * @param value the value to set.
   */
  public void setStart(long value) {
    this.start = value;
  }

  /**
   * Gets the value of the 'stop' field.
   * @return The value of the 'stop' field.
   */
  public long getStop() {
    return stop;
  }


  /**
   * Sets the value of the 'stop' field.
   * @param value the value to set.
   */
  public void setStop(long value) {
    this.stop = value;
  }

  /**
   * Gets the value of the 'strand' field.
   * @return The value of the 'strand' field.
   */
  public java.lang.CharSequence getStrand() {
    return strand;
  }


  /**
   * Sets the value of the 'strand' field.
   * @param value the value to set.
   */
  public void setStrand(java.lang.CharSequence value) {
    this.strand = value;
  }

  /**
   * Creates a new gCoords RecordBuilder.
   * @return A new gCoords RecordBuilder
   */
  public static it.polimi.genomics.avro.garray.gCoords.Builder newBuilder() {
    return new it.polimi.genomics.avro.garray.gCoords.Builder();
  }

  /**
   * Creates a new gCoords RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new gCoords RecordBuilder
   */
  public static it.polimi.genomics.avro.garray.gCoords.Builder newBuilder(it.polimi.genomics.avro.garray.gCoords.Builder other) {
    if (other == null) {
      return new it.polimi.genomics.avro.garray.gCoords.Builder();
    } else {
      return new it.polimi.genomics.avro.garray.gCoords.Builder(other);
    }
  }

  /**
   * Creates a new gCoords RecordBuilder by copying an existing gCoords instance.
   * @param other The existing instance to copy.
   * @return A new gCoords RecordBuilder
   */
  public static it.polimi.genomics.avro.garray.gCoords.Builder newBuilder(it.polimi.genomics.avro.garray.gCoords other) {
    if (other == null) {
      return new it.polimi.genomics.avro.garray.gCoords.Builder();
    } else {
      return new it.polimi.genomics.avro.garray.gCoords.Builder(other);
    }
  }

  /**
   * RecordBuilder for gCoords instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<gCoords>
    implements org.apache.avro.data.RecordBuilder<gCoords> {

    private java.lang.CharSequence chr;
    private long start;
    private long stop;
    private java.lang.CharSequence strand;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(it.polimi.genomics.avro.garray.gCoords.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.chr)) {
        this.chr = data().deepCopy(fields()[0].schema(), other.chr);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.start)) {
        this.start = data().deepCopy(fields()[1].schema(), other.start);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.stop)) {
        this.stop = data().deepCopy(fields()[2].schema(), other.stop);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.strand)) {
        this.strand = data().deepCopy(fields()[3].schema(), other.strand);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
    }

    /**
     * Creates a Builder by copying an existing gCoords instance
     * @param other The existing instance to copy.
     */
    private Builder(it.polimi.genomics.avro.garray.gCoords other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.chr)) {
        this.chr = data().deepCopy(fields()[0].schema(), other.chr);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.start)) {
        this.start = data().deepCopy(fields()[1].schema(), other.start);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.stop)) {
        this.stop = data().deepCopy(fields()[2].schema(), other.stop);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.strand)) {
        this.strand = data().deepCopy(fields()[3].schema(), other.strand);
        fieldSetFlags()[3] = true;
      }
    }

    /**
      * Gets the value of the 'chr' field.
      * @return The value.
      */
    public java.lang.CharSequence getChr() {
      return chr;
    }


    /**
      * Sets the value of the 'chr' field.
      * @param value The value of 'chr'.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray.gCoords.Builder setChr(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.chr = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'chr' field has been set.
      * @return True if the 'chr' field has been set, false otherwise.
      */
    public boolean hasChr() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'chr' field.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray.gCoords.Builder clearChr() {
      chr = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'start' field.
      * @return The value.
      */
    public long getStart() {
      return start;
    }


    /**
      * Sets the value of the 'start' field.
      * @param value The value of 'start'.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray.gCoords.Builder setStart(long value) {
      validate(fields()[1], value);
      this.start = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'start' field has been set.
      * @return True if the 'start' field has been set, false otherwise.
      */
    public boolean hasStart() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'start' field.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray.gCoords.Builder clearStart() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'stop' field.
      * @return The value.
      */
    public long getStop() {
      return stop;
    }


    /**
      * Sets the value of the 'stop' field.
      * @param value The value of 'stop'.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray.gCoords.Builder setStop(long value) {
      validate(fields()[2], value);
      this.stop = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'stop' field has been set.
      * @return True if the 'stop' field has been set, false otherwise.
      */
    public boolean hasStop() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'stop' field.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray.gCoords.Builder clearStop() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'strand' field.
      * @return The value.
      */
    public java.lang.CharSequence getStrand() {
      return strand;
    }


    /**
      * Sets the value of the 'strand' field.
      * @param value The value of 'strand'.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray.gCoords.Builder setStrand(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.strand = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'strand' field has been set.
      * @return True if the 'strand' field has been set, false otherwise.
      */
    public boolean hasStrand() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'strand' field.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray.gCoords.Builder clearStrand() {
      strand = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public gCoords build() {
      try {
        gCoords record = new gCoords();
        record.chr = fieldSetFlags()[0] ? this.chr : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.start = fieldSetFlags()[1] ? this.start : (java.lang.Long) defaultValue(fields()[1]);
        record.stop = fieldSetFlags()[2] ? this.stop : (java.lang.Long) defaultValue(fields()[2]);
        record.strand = fieldSetFlags()[3] ? this.strand : (java.lang.CharSequence) defaultValue(fields()[3]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<gCoords>
    WRITER$ = (org.apache.avro.io.DatumWriter<gCoords>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<gCoords>
    READER$ = (org.apache.avro.io.DatumReader<gCoords>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.chr);

    out.writeLong(this.start);

    out.writeLong(this.stop);

    out.writeString(this.strand);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.chr = in.readString(this.chr instanceof Utf8 ? (Utf8)this.chr : null);

      this.start = in.readLong();

      this.stop = in.readLong();

      this.strand = in.readString(this.strand instanceof Utf8 ? (Utf8)this.strand : null);

    } else {
      for (int i = 0; i < 4; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.chr = in.readString(this.chr instanceof Utf8 ? (Utf8)this.chr : null);
          break;

        case 1:
          this.start = in.readLong();
          break;

        case 2:
          this.stop = in.readLong();
          break;

        case 3:
          this.strand = in.readString(this.strand instanceof Utf8 ? (Utf8)this.strand : null);
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










