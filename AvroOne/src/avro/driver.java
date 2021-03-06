/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package avro;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class driver extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 4499222567385708536L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"driver\",\"namespace\":\"avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"hours\",\"type\":\"int\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"address\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence name;
  @Deprecated public int id;
  @Deprecated public int hours;
  @Deprecated public int age;
  @Deprecated public java.lang.CharSequence address;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public driver() {}

  /**
   * All-args constructor.
   * @param name The new value for name
   * @param id The new value for id
   * @param hours The new value for hours
   * @param age The new value for age
   * @param address The new value for address
   */
  public driver(java.lang.CharSequence name, java.lang.Integer id, java.lang.Integer hours, java.lang.Integer age, java.lang.CharSequence address) {
    this.name = name;
    this.id = id;
    this.hours = hours;
    this.age = age;
    this.address = address;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return name;
    case 1: return id;
    case 2: return hours;
    case 3: return age;
    case 4: return address;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: name = (java.lang.CharSequence)value$; break;
    case 1: id = (java.lang.Integer)value$; break;
    case 2: hours = (java.lang.Integer)value$; break;
    case 3: age = (java.lang.Integer)value$; break;
    case 4: address = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'name' field.
   * @return The value of the 'name' field.
   */
  public java.lang.CharSequence getName() {
    return name;
  }

  /**
   * Sets the value of the 'name' field.
   * @param value the value to set.
   */
  public void setName(java.lang.CharSequence value) {
    this.name = value;
  }

  /**
   * Gets the value of the 'id' field.
   * @return The value of the 'id' field.
   */
  public java.lang.Integer getId() {
    return id;
  }

  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(java.lang.Integer value) {
    this.id = value;
  }

  /**
   * Gets the value of the 'hours' field.
   * @return The value of the 'hours' field.
   */
  public java.lang.Integer getHours() {
    return hours;
  }

  /**
   * Sets the value of the 'hours' field.
   * @param value the value to set.
   */
  public void setHours(java.lang.Integer value) {
    this.hours = value;
  }

  /**
   * Gets the value of the 'age' field.
   * @return The value of the 'age' field.
   */
  public java.lang.Integer getAge() {
    return age;
  }

  /**
   * Sets the value of the 'age' field.
   * @param value the value to set.
   */
  public void setAge(java.lang.Integer value) {
    this.age = value;
  }

  /**
   * Gets the value of the 'address' field.
   * @return The value of the 'address' field.
   */
  public java.lang.CharSequence getAddress() {
    return address;
  }

  /**
   * Sets the value of the 'address' field.
   * @param value the value to set.
   */
  public void setAddress(java.lang.CharSequence value) {
    this.address = value;
  }

  /**
   * Creates a new driver RecordBuilder.
   * @return A new driver RecordBuilder
   */
  public static avro.driver.Builder newBuilder() {
    return new avro.driver.Builder();
  }

  /**
   * Creates a new driver RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new driver RecordBuilder
   */
  public static avro.driver.Builder newBuilder(avro.driver.Builder other) {
    return new avro.driver.Builder(other);
  }

  /**
   * Creates a new driver RecordBuilder by copying an existing driver instance.
   * @param other The existing instance to copy.
   * @return A new driver RecordBuilder
   */
  public static avro.driver.Builder newBuilder(avro.driver other) {
    return new avro.driver.Builder(other);
  }

  /**
   * RecordBuilder for driver instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<driver>
    implements org.apache.avro.data.RecordBuilder<driver> {

    private java.lang.CharSequence name;
    private int id;
    private int hours;
    private int age;
    private java.lang.CharSequence address;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(avro.driver.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.id)) {
        this.id = data().deepCopy(fields()[1].schema(), other.id);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.hours)) {
        this.hours = data().deepCopy(fields()[2].schema(), other.hours);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.age)) {
        this.age = data().deepCopy(fields()[3].schema(), other.age);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.address)) {
        this.address = data().deepCopy(fields()[4].schema(), other.address);
        fieldSetFlags()[4] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing driver instance
     * @param other The existing instance to copy.
     */
    private Builder(avro.driver other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.name)) {
        this.name = data().deepCopy(fields()[0].schema(), other.name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.id)) {
        this.id = data().deepCopy(fields()[1].schema(), other.id);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.hours)) {
        this.hours = data().deepCopy(fields()[2].schema(), other.hours);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.age)) {
        this.age = data().deepCopy(fields()[3].schema(), other.age);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.address)) {
        this.address = data().deepCopy(fields()[4].schema(), other.address);
        fieldSetFlags()[4] = true;
      }
    }

    /**
      * Gets the value of the 'name' field.
      * @return The value.
      */
    public java.lang.CharSequence getName() {
      return name;
    }

    /**
      * Sets the value of the 'name' field.
      * @param value The value of 'name'.
      * @return This builder.
      */
    public avro.driver.Builder setName(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.name = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'name' field has been set.
      * @return True if the 'name' field has been set, false otherwise.
      */
    public boolean hasName() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'name' field.
      * @return This builder.
      */
    public avro.driver.Builder clearName() {
      name = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'id' field.
      * @return The value.
      */
    public java.lang.Integer getId() {
      return id;
    }

    /**
      * Sets the value of the 'id' field.
      * @param value The value of 'id'.
      * @return This builder.
      */
    public avro.driver.Builder setId(int value) {
      validate(fields()[1], value);
      this.id = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'id' field has been set.
      * @return True if the 'id' field has been set, false otherwise.
      */
    public boolean hasId() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'id' field.
      * @return This builder.
      */
    public avro.driver.Builder clearId() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'hours' field.
      * @return The value.
      */
    public java.lang.Integer getHours() {
      return hours;
    }

    /**
      * Sets the value of the 'hours' field.
      * @param value The value of 'hours'.
      * @return This builder.
      */
    public avro.driver.Builder setHours(int value) {
      validate(fields()[2], value);
      this.hours = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'hours' field has been set.
      * @return True if the 'hours' field has been set, false otherwise.
      */
    public boolean hasHours() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'hours' field.
      * @return This builder.
      */
    public avro.driver.Builder clearHours() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'age' field.
      * @return The value.
      */
    public java.lang.Integer getAge() {
      return age;
    }

    /**
      * Sets the value of the 'age' field.
      * @param value The value of 'age'.
      * @return This builder.
      */
    public avro.driver.Builder setAge(int value) {
      validate(fields()[3], value);
      this.age = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'age' field has been set.
      * @return True if the 'age' field has been set, false otherwise.
      */
    public boolean hasAge() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'age' field.
      * @return This builder.
      */
    public avro.driver.Builder clearAge() {
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'address' field.
      * @return The value.
      */
    public java.lang.CharSequence getAddress() {
      return address;
    }

    /**
      * Sets the value of the 'address' field.
      * @param value The value of 'address'.
      * @return This builder.
      */
    public avro.driver.Builder setAddress(java.lang.CharSequence value) {
      validate(fields()[4], value);
      this.address = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'address' field has been set.
      * @return True if the 'address' field has been set, false otherwise.
      */
    public boolean hasAddress() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'address' field.
      * @return This builder.
      */
    public avro.driver.Builder clearAddress() {
      address = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    public driver build() {
      try {
        driver record = new driver();
        record.name = fieldSetFlags()[0] ? this.name : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.id = fieldSetFlags()[1] ? this.id : (java.lang.Integer) defaultValue(fields()[1]);
        record.hours = fieldSetFlags()[2] ? this.hours : (java.lang.Integer) defaultValue(fields()[2]);
        record.age = fieldSetFlags()[3] ? this.age : (java.lang.Integer) defaultValue(fields()[3]);
        record.address = fieldSetFlags()[4] ? this.address : (java.lang.CharSequence) defaultValue(fields()[4]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
