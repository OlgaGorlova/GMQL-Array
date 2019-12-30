/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package it.polimi.genomics.avro.core;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class GString extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 3965607427013981431L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"GString\",\"namespace\":\"it.polimi.genomics.avro.core\",\"fields\":[{\"name\":\"v\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<GString> ENCODER =
      new BinaryMessageEncoder<GString>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<GString> DECODER =
      new BinaryMessageDecoder<GString>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<GString> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<GString> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<GString> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<GString>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this GString to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a GString from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a GString instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static GString fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private java.lang.CharSequence v;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public GString() {}

  /**
   * All-args constructor.
   * @param v The new value for v
   */
  public GString(java.lang.CharSequence v) {
    this.v = v;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return v;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: v = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'v' field.
   * @return The value of the 'v' field.
   */
  public java.lang.CharSequence getV() {
    return v;
  }


  /**
   * Sets the value of the 'v' field.
   * @param value the value to set.
   */
  public void setV(java.lang.CharSequence value) {
    this.v = value;
  }

  /**
   * Creates a new GString RecordBuilder.
   * @return A new GString RecordBuilder
   */
  public static it.polimi.genomics.avro.core.GString.Builder newBuilder() {
    return new it.polimi.genomics.avro.core.GString.Builder();
  }

  /**
   * Creates a new GString RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new GString RecordBuilder
   */
  public static it.polimi.genomics.avro.core.GString.Builder newBuilder(it.polimi.genomics.avro.core.GString.Builder other) {
    if (other == null) {
      return new it.polimi.genomics.avro.core.GString.Builder();
    } else {
      return new it.polimi.genomics.avro.core.GString.Builder(other);
    }
  }

  /**
   * Creates a new GString RecordBuilder by copying an existing GString instance.
   * @param other The existing instance to copy.
   * @return A new GString RecordBuilder
   */
  public static it.polimi.genomics.avro.core.GString.Builder newBuilder(it.polimi.genomics.avro.core.GString other) {
    if (other == null) {
      return new it.polimi.genomics.avro.core.GString.Builder();
    } else {
      return new it.polimi.genomics.avro.core.GString.Builder(other);
    }
  }

  /**
   * RecordBuilder for GString instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<GString>
    implements org.apache.avro.data.RecordBuilder<GString> {

    private java.lang.CharSequence v;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(it.polimi.genomics.avro.core.GString.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.v)) {
        this.v = data().deepCopy(fields()[0].schema(), other.v);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
    }

    /**
     * Creates a Builder by copying an existing GString instance
     * @param other The existing instance to copy.
     */
    private Builder(it.polimi.genomics.avro.core.GString other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.v)) {
        this.v = data().deepCopy(fields()[0].schema(), other.v);
        fieldSetFlags()[0] = true;
      }
    }

    /**
      * Gets the value of the 'v' field.
      * @return The value.
      */
    public java.lang.CharSequence getV() {
      return v;
    }


    /**
      * Sets the value of the 'v' field.
      * @param value The value of 'v'.
      * @return This builder.
      */
    public it.polimi.genomics.avro.core.GString.Builder setV(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.v = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'v' field has been set.
      * @return True if the 'v' field has been set, false otherwise.
      */
    public boolean hasV() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'v' field.
      * @return This builder.
      */
    public it.polimi.genomics.avro.core.GString.Builder clearV() {
      v = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public GString build() {
      try {
        GString record = new GString();
        record.v = fieldSetFlags()[0] ? this.v : (java.lang.CharSequence) defaultValue(fields()[0]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<GString>
    WRITER$ = (org.apache.avro.io.DatumWriter<GString>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<GString>
    READER$ = (org.apache.avro.io.DatumReader<GString>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.v);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.v = in.readString(this.v instanceof Utf8 ? (Utf8)this.v : null);

    } else {
      for (int i = 0; i < 1; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.v = in.readString(this.v instanceof Utf8 ? (Utf8)this.v : null);
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










