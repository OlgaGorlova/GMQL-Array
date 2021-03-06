/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package it.polimi.genomics.avro.myavro;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class repRec extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 3110016480912507321L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"repRec\",\"namespace\":\"it.polimi.genomics.avro.myavro\",\"fields\":[{\"name\":\"repArray\",\"type\":{\"type\":\"array\",\"items\":\"double\"}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<repRec> ENCODER =
      new BinaryMessageEncoder<repRec>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<repRec> DECODER =
      new BinaryMessageDecoder<repRec>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<repRec> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<repRec> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<repRec> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<repRec>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this repRec to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a repRec from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a repRec instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static repRec fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private java.util.List<java.lang.Double> repArray;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public repRec() {}

  /**
   * All-args constructor.
   * @param repArray The new value for repArray
   */
  public repRec(java.util.List<java.lang.Double> repArray) {
    this.repArray = repArray;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return repArray;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: repArray = (java.util.List<java.lang.Double>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'repArray' field.
   * @return The value of the 'repArray' field.
   */
  public java.util.List<java.lang.Double> getRepArray() {
    return repArray;
  }


  /**
   * Sets the value of the 'repArray' field.
   * @param value the value to set.
   */
  public void setRepArray(java.util.List<java.lang.Double> value) {
    this.repArray = value;
  }

  /**
   * Creates a new repRec RecordBuilder.
   * @return A new repRec RecordBuilder
   */
  public static it.polimi.genomics.avro.myavro.repRec.Builder newBuilder() {
    return new it.polimi.genomics.avro.myavro.repRec.Builder();
  }

  /**
   * Creates a new repRec RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new repRec RecordBuilder
   */
  public static it.polimi.genomics.avro.myavro.repRec.Builder newBuilder(it.polimi.genomics.avro.myavro.repRec.Builder other) {
    if (other == null) {
      return new it.polimi.genomics.avro.myavro.repRec.Builder();
    } else {
      return new it.polimi.genomics.avro.myavro.repRec.Builder(other);
    }
  }

  /**
   * Creates a new repRec RecordBuilder by copying an existing repRec instance.
   * @param other The existing instance to copy.
   * @return A new repRec RecordBuilder
   */
  public static it.polimi.genomics.avro.myavro.repRec.Builder newBuilder(it.polimi.genomics.avro.myavro.repRec other) {
    if (other == null) {
      return new it.polimi.genomics.avro.myavro.repRec.Builder();
    } else {
      return new it.polimi.genomics.avro.myavro.repRec.Builder(other);
    }
  }

  /**
   * RecordBuilder for repRec instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<repRec>
    implements org.apache.avro.data.RecordBuilder<repRec> {

    private java.util.List<java.lang.Double> repArray;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(it.polimi.genomics.avro.myavro.repRec.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.repArray)) {
        this.repArray = data().deepCopy(fields()[0].schema(), other.repArray);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
    }

    /**
     * Creates a Builder by copying an existing repRec instance
     * @param other The existing instance to copy.
     */
    private Builder(it.polimi.genomics.avro.myavro.repRec other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.repArray)) {
        this.repArray = data().deepCopy(fields()[0].schema(), other.repArray);
        fieldSetFlags()[0] = true;
      }
    }

    /**
      * Gets the value of the 'repArray' field.
      * @return The value.
      */
    public java.util.List<java.lang.Double> getRepArray() {
      return repArray;
    }


    /**
      * Sets the value of the 'repArray' field.
      * @param value The value of 'repArray'.
      * @return This builder.
      */
    public it.polimi.genomics.avro.myavro.repRec.Builder setRepArray(java.util.List<java.lang.Double> value) {
      validate(fields()[0], value);
      this.repArray = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'repArray' field has been set.
      * @return True if the 'repArray' field has been set, false otherwise.
      */
    public boolean hasRepArray() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'repArray' field.
      * @return This builder.
      */
    public it.polimi.genomics.avro.myavro.repRec.Builder clearRepArray() {
      repArray = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public repRec build() {
      try {
        repRec record = new repRec();
        record.repArray = fieldSetFlags()[0] ? this.repArray : (java.util.List<java.lang.Double>) defaultValue(fields()[0]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<repRec>
    WRITER$ = (org.apache.avro.io.DatumWriter<repRec>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<repRec>
    READER$ = (org.apache.avro.io.DatumReader<repRec>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    long size0 = this.repArray.size();
    out.writeArrayStart();
    out.setItemCount(size0);
    long actualSize0 = 0;
    for (java.lang.Double e0: this.repArray) {
      actualSize0++;
      out.startItem();
      out.writeDouble(e0);
    }
    out.writeArrayEnd();
    if (actualSize0 != size0)
      throw new java.util.ConcurrentModificationException("Array-size written was " + size0 + ", but element count was " + actualSize0 + ".");

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      long size0 = in.readArrayStart();
      java.util.List<java.lang.Double> a0 = this.repArray;
      if (a0 == null) {
        a0 = new SpecificData.Array<java.lang.Double>((int)size0, SCHEMA$.getField("repArray").schema());
        this.repArray = a0;
      } else a0.clear();
      SpecificData.Array<java.lang.Double> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<java.lang.Double>)a0 : null);
      for ( ; 0 < size0; size0 = in.arrayNext()) {
        for ( ; size0 != 0; size0--) {
          java.lang.Double e0 = (ga0 != null ? ga0.peek() : null);
          e0 = in.readDouble();
          a0.add(e0);
        }
      }

    } else {
      for (int i = 0; i < 1; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          long size0 = in.readArrayStart();
          java.util.List<java.lang.Double> a0 = this.repArray;
          if (a0 == null) {
            a0 = new SpecificData.Array<java.lang.Double>((int)size0, SCHEMA$.getField("repArray").schema());
            this.repArray = a0;
          } else a0.clear();
          SpecificData.Array<java.lang.Double> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<java.lang.Double>)a0 : null);
          for ( ; 0 < size0; size0 = in.arrayNext()) {
            for ( ; size0 != 0; size0--) {
              java.lang.Double e0 = (ga0 != null ? ga0.peek() : null);
              e0 = in.readDouble();
              a0.add(e0);
            }
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










