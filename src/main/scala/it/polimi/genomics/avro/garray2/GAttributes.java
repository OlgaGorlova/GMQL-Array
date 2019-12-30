/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package it.polimi.genomics.avro.garray2;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class GAttributes extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 3160205349280761361L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"GAttributes\",\"namespace\":\"it.polimi.genomics.avro.garray2\",\"fields\":[{\"name\":\"samples\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"idsRec\",\"fields\":[{\"name\":\"_1\",\"type\":\"long\"},{\"name\":\"_2\",\"type\":\"int\"}]}}},{\"name\":\"att\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"GDouble\",\"namespace\":\"it.polimi.genomics.avro.core\",\"fields\":[{\"name\":\"v\",\"type\":\"double\"}]},{\"type\":\"record\",\"name\":\"GInt\",\"namespace\":\"it.polimi.genomics.avro.core\",\"fields\":[{\"name\":\"v\",\"type\":\"int\"}]},{\"type\":\"record\",\"name\":\"GNull\",\"namespace\":\"it.polimi.genomics.avro.core\",\"fields\":[]},{\"type\":\"record\",\"name\":\"GString\",\"namespace\":\"it.polimi.genomics.avro.core\",\"fields\":[{\"name\":\"v\",\"type\":\"string\"}]}]}}}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<GAttributes> ENCODER =
      new BinaryMessageEncoder<GAttributes>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<GAttributes> DECODER =
      new BinaryMessageDecoder<GAttributes>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<GAttributes> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<GAttributes> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<GAttributes> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<GAttributes>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this GAttributes to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a GAttributes from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a GAttributes instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static GAttributes fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private java.util.List<it.polimi.genomics.avro.garray2.idsRec> samples;
   private java.util.List<java.util.List<java.util.List<java.lang.Object>>> att;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public GAttributes() {}

  /**
   * All-args constructor.
   * @param samples The new value for samples
   * @param att The new value for att
   */
  public GAttributes(java.util.List<it.polimi.genomics.avro.garray2.idsRec> samples, java.util.List<java.util.List<java.util.List<java.lang.Object>>> att) {
    this.samples = samples;
    this.att = att;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return samples;
    case 1: return att;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: samples = (java.util.List<it.polimi.genomics.avro.garray2.idsRec>)value$; break;
    case 1: att = (java.util.List<java.util.List<java.util.List<java.lang.Object>>>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'samples' field.
   * @return The value of the 'samples' field.
   */
  public java.util.List<it.polimi.genomics.avro.garray2.idsRec> getSamples() {
    return samples;
  }


  /**
   * Sets the value of the 'samples' field.
   * @param value the value to set.
   */
  public void setSamples(java.util.List<it.polimi.genomics.avro.garray2.idsRec> value) {
    this.samples = value;
  }

  /**
   * Gets the value of the 'att' field.
   * @return The value of the 'att' field.
   */
  public java.util.List<java.util.List<java.util.List<java.lang.Object>>> getAtt() {
    return att;
  }


  /**
   * Sets the value of the 'att' field.
   * @param value the value to set.
   */
  public void setAtt(java.util.List<java.util.List<java.util.List<java.lang.Object>>> value) {
    this.att = value;
  }

  /**
   * Creates a new GAttributes RecordBuilder.
   * @return A new GAttributes RecordBuilder
   */
  public static it.polimi.genomics.avro.garray2.GAttributes.Builder newBuilder() {
    return new it.polimi.genomics.avro.garray2.GAttributes.Builder();
  }

  /**
   * Creates a new GAttributes RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new GAttributes RecordBuilder
   */
  public static it.polimi.genomics.avro.garray2.GAttributes.Builder newBuilder(it.polimi.genomics.avro.garray2.GAttributes.Builder other) {
    if (other == null) {
      return new it.polimi.genomics.avro.garray2.GAttributes.Builder();
    } else {
      return new it.polimi.genomics.avro.garray2.GAttributes.Builder(other);
    }
  }

  /**
   * Creates a new GAttributes RecordBuilder by copying an existing GAttributes instance.
   * @param other The existing instance to copy.
   * @return A new GAttributes RecordBuilder
   */
  public static it.polimi.genomics.avro.garray2.GAttributes.Builder newBuilder(it.polimi.genomics.avro.garray2.GAttributes other) {
    if (other == null) {
      return new it.polimi.genomics.avro.garray2.GAttributes.Builder();
    } else {
      return new it.polimi.genomics.avro.garray2.GAttributes.Builder(other);
    }
  }

  /**
   * RecordBuilder for GAttributes instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<GAttributes>
    implements org.apache.avro.data.RecordBuilder<GAttributes> {

    private java.util.List<it.polimi.genomics.avro.garray2.idsRec> samples;
    private java.util.List<java.util.List<java.util.List<java.lang.Object>>> att;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(it.polimi.genomics.avro.garray2.GAttributes.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.samples)) {
        this.samples = data().deepCopy(fields()[0].schema(), other.samples);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.att)) {
        this.att = data().deepCopy(fields()[1].schema(), other.att);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
    }

    /**
     * Creates a Builder by copying an existing GAttributes instance
     * @param other The existing instance to copy.
     */
    private Builder(it.polimi.genomics.avro.garray2.GAttributes other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.samples)) {
        this.samples = data().deepCopy(fields()[0].schema(), other.samples);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.att)) {
        this.att = data().deepCopy(fields()[1].schema(), other.att);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'samples' field.
      * @return The value.
      */
    public java.util.List<it.polimi.genomics.avro.garray2.idsRec> getSamples() {
      return samples;
    }


    /**
      * Sets the value of the 'samples' field.
      * @param value The value of 'samples'.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray2.GAttributes.Builder setSamples(java.util.List<it.polimi.genomics.avro.garray2.idsRec> value) {
      validate(fields()[0], value);
      this.samples = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'samples' field has been set.
      * @return True if the 'samples' field has been set, false otherwise.
      */
    public boolean hasSamples() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'samples' field.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray2.GAttributes.Builder clearSamples() {
      samples = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'att' field.
      * @return The value.
      */
    public java.util.List<java.util.List<java.util.List<java.lang.Object>>> getAtt() {
      return att;
    }


    /**
      * Sets the value of the 'att' field.
      * @param value The value of 'att'.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray2.GAttributes.Builder setAtt(java.util.List<java.util.List<java.util.List<java.lang.Object>>> value) {
      validate(fields()[1], value);
      this.att = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'att' field has been set.
      * @return True if the 'att' field has been set, false otherwise.
      */
    public boolean hasAtt() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'att' field.
      * @return This builder.
      */
    public it.polimi.genomics.avro.garray2.GAttributes.Builder clearAtt() {
      att = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public GAttributes build() {
      try {
        GAttributes record = new GAttributes();
        record.samples = fieldSetFlags()[0] ? this.samples : (java.util.List<it.polimi.genomics.avro.garray2.idsRec>) defaultValue(fields()[0]);
        record.att = fieldSetFlags()[1] ? this.att : (java.util.List<java.util.List<java.util.List<java.lang.Object>>>) defaultValue(fields()[1]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<GAttributes>
    WRITER$ = (org.apache.avro.io.DatumWriter<GAttributes>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<GAttributes>
    READER$ = (org.apache.avro.io.DatumReader<GAttributes>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}










