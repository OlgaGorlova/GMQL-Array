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
public class GNull extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 6860488334651986515L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"GNull\",\"namespace\":\"it.polimi.genomics.avro.core\",\"fields\":[]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<GNull> ENCODER =
      new BinaryMessageEncoder<GNull>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<GNull> DECODER =
      new BinaryMessageDecoder<GNull>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<GNull> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<GNull> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<GNull> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<GNull>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this GNull to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a GNull from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a GNull instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static GNull fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }


  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Creates a new GNull RecordBuilder.
   * @return A new GNull RecordBuilder
   */
  public static it.polimi.genomics.avro.core.GNull.Builder newBuilder() {
    return new it.polimi.genomics.avro.core.GNull.Builder();
  }

  /**
   * Creates a new GNull RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new GNull RecordBuilder
   */
  public static it.polimi.genomics.avro.core.GNull.Builder newBuilder(it.polimi.genomics.avro.core.GNull.Builder other) {
    if (other == null) {
      return new it.polimi.genomics.avro.core.GNull.Builder();
    } else {
      return new it.polimi.genomics.avro.core.GNull.Builder(other);
    }
  }

  /**
   * Creates a new GNull RecordBuilder by copying an existing GNull instance.
   * @param other The existing instance to copy.
   * @return A new GNull RecordBuilder
   */
  public static it.polimi.genomics.avro.core.GNull.Builder newBuilder(it.polimi.genomics.avro.core.GNull other) {
    if (other == null) {
      return new it.polimi.genomics.avro.core.GNull.Builder();
    } else {
      return new it.polimi.genomics.avro.core.GNull.Builder(other);
    }
  }

  /**
   * RecordBuilder for GNull instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<GNull>
    implements org.apache.avro.data.RecordBuilder<GNull> {


    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(it.polimi.genomics.avro.core.GNull.Builder other) {
      super(other);
    }

    /**
     * Creates a Builder by copying an existing GNull instance
     * @param other The existing instance to copy.
     */
    private Builder(it.polimi.genomics.avro.core.GNull other) {
      super(SCHEMA$);
    }

    @Override
    @SuppressWarnings("unchecked")
    public GNull build() {
      try {
        GNull record = new GNull();
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<GNull>
    WRITER$ = (org.apache.avro.io.DatumWriter<GNull>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<GNull>
    READER$ = (org.apache.avro.io.DatumReader<GNull>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
    } else {
      for (int i = 0; i < 0; i++) {
        switch (fieldOrder[i].pos()) {
        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










