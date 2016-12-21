/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.refactorlabs.cs378.sessions;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Session extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -1635465549709010106L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Session\",\"namespace\":\"com.refactorlabs.cs378.sessions\",\"fields\":[{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"events\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"event_type\",\"type\":{\"type\":\"enum\",\"name\":\"EventType\",\"symbols\":[\"CHANGE\",\"CLICK\",\"DISPLAY\",\"EDIT\",\"SHOW\",\"VISIT\"]}},{\"name\":\"event_subtype\",\"type\":{\"type\":\"enum\",\"name\":\"EventSubtype\",\"symbols\":[\"CONTACT_FORM\",\"ALTERNATIVE\",\"CONTACT_BUTTON\",\"FEATURES\",\"GET_DIRECTIONS\",\"VEHICLE_HISTORY\",\"BADGE_DETAIL\",\"PHOTO_MODAL\",\"BADGES\",\"MARKET_REPORT\"]}},{\"name\":\"event_time\",\"type\":[\"null\",\"string\"]},{\"name\":\"page\",\"type\":[\"null\",\"string\"]},{\"name\":\"referring_domain\",\"type\":[\"null\",\"string\"]},{\"name\":\"city\",\"type\":[\"null\",\"string\"]},{\"name\":\"vin\",\"type\":[\"null\",\"string\"]},{\"name\":\"condition\",\"type\":{\"type\":\"enum\",\"name\":\"Condition\",\"symbols\":[\"New\",\"Used\"]}},{\"name\":\"year\",\"type\":\"long\"},{\"name\":\"make\",\"type\":[\"null\",\"string\"]},{\"name\":\"model\",\"type\":[\"null\",\"string\"]},{\"name\":\"trim\",\"type\":[\"null\",\"string\"]},{\"name\":\"body_style\",\"type\":{\"type\":\"enum\",\"name\":\"BodyStyle\",\"symbols\":[\"Convertible\",\"Coupe\",\"Hatchback\",\"Minivan\",\"Pickup\",\"SUV\",\"Sedan\",\"Van\",\"Wagon\"]}},{\"name\":\"cab_style\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"CabStyle\",\"symbols\":[\"Crew\",\"Extended\",\"Regular\"]}]},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"mileage\",\"type\":\"long\"},{\"name\":\"image_count\",\"type\":\"long\"},{\"name\":\"free_carfax_report\",\"type\":\"boolean\"},{\"name\":\"features\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"string\"}]}]}}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence user_id;
  @Deprecated public java.util.List<com.refactorlabs.cs378.sessions.Event> events;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Session() {}

  /**
   * All-args constructor.
   * @param user_id The new value for user_id
   * @param events The new value for events
   */
  public Session(java.lang.CharSequence user_id, java.util.List<com.refactorlabs.cs378.sessions.Event> events) {
    this.user_id = user_id;
    this.events = events;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return user_id;
    case 1: return events;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: user_id = (java.lang.CharSequence)value$; break;
    case 1: events = (java.util.List<com.refactorlabs.cs378.sessions.Event>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'user_id' field.
   * @return The value of the 'user_id' field.
   */
  public java.lang.CharSequence getUserId() {
    return user_id;
  }

  /**
   * Sets the value of the 'user_id' field.
   * @param value the value to set.
   */
  public void setUserId(java.lang.CharSequence value) {
    this.user_id = value;
  }

  /**
   * Gets the value of the 'events' field.
   * @return The value of the 'events' field.
   */
  public java.util.List<com.refactorlabs.cs378.sessions.Event> getEvents() {
    return events;
  }

  /**
   * Sets the value of the 'events' field.
   * @param value the value to set.
   */
  public void setEvents(java.util.List<com.refactorlabs.cs378.sessions.Event> value) {
    this.events = value;
  }

  /**
   * Creates a new Session RecordBuilder.
   * @return A new Session RecordBuilder
   */
  public static com.refactorlabs.cs378.sessions.Session.Builder newBuilder() {
    return new com.refactorlabs.cs378.sessions.Session.Builder();
  }

  /**
   * Creates a new Session RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Session RecordBuilder
   */
  public static com.refactorlabs.cs378.sessions.Session.Builder newBuilder(com.refactorlabs.cs378.sessions.Session.Builder other) {
    return new com.refactorlabs.cs378.sessions.Session.Builder(other);
  }

  /**
   * Creates a new Session RecordBuilder by copying an existing Session instance.
   * @param other The existing instance to copy.
   * @return A new Session RecordBuilder
   */
  public static com.refactorlabs.cs378.sessions.Session.Builder newBuilder(com.refactorlabs.cs378.sessions.Session other) {
    return new com.refactorlabs.cs378.sessions.Session.Builder(other);
  }

  /**
   * RecordBuilder for Session instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Session>
    implements org.apache.avro.data.RecordBuilder<Session> {

    private java.lang.CharSequence user_id;
    private java.util.List<com.refactorlabs.cs378.sessions.Event> events;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.refactorlabs.cs378.sessions.Session.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.user_id)) {
        this.user_id = data().deepCopy(fields()[0].schema(), other.user_id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.events)) {
        this.events = data().deepCopy(fields()[1].schema(), other.events);
        fieldSetFlags()[1] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing Session instance
     * @param other The existing instance to copy.
     */
    private Builder(com.refactorlabs.cs378.sessions.Session other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.user_id)) {
        this.user_id = data().deepCopy(fields()[0].schema(), other.user_id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.events)) {
        this.events = data().deepCopy(fields()[1].schema(), other.events);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'user_id' field.
      * @return The value.
      */
    public java.lang.CharSequence getUserId() {
      return user_id;
    }

    /**
      * Sets the value of the 'user_id' field.
      * @param value The value of 'user_id'.
      * @return This builder.
      */
    public com.refactorlabs.cs378.sessions.Session.Builder setUserId(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.user_id = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'user_id' field has been set.
      * @return True if the 'user_id' field has been set, false otherwise.
      */
    public boolean hasUserId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'user_id' field.
      * @return This builder.
      */
    public com.refactorlabs.cs378.sessions.Session.Builder clearUserId() {
      user_id = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'events' field.
      * @return The value.
      */
    public java.util.List<com.refactorlabs.cs378.sessions.Event> getEvents() {
      return events;
    }

    /**
      * Sets the value of the 'events' field.
      * @param value The value of 'events'.
      * @return This builder.
      */
    public com.refactorlabs.cs378.sessions.Session.Builder setEvents(java.util.List<com.refactorlabs.cs378.sessions.Event> value) {
      validate(fields()[1], value);
      this.events = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'events' field has been set.
      * @return True if the 'events' field has been set, false otherwise.
      */
    public boolean hasEvents() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'events' field.
      * @return This builder.
      */
    public com.refactorlabs.cs378.sessions.Session.Builder clearEvents() {
      events = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public Session build() {
      try {
        Session record = new Session();
        record.user_id = fieldSetFlags()[0] ? this.user_id : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.events = fieldSetFlags()[1] ? this.events : (java.util.List<com.refactorlabs.cs378.sessions.Event>) defaultValue(fields()[1]);
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