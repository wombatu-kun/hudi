/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.avro;

import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.exception.HoodieJsonToAvroConversionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_ENCODED_DECIMAL_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestMercifulJsonConverter extends MercifulJsonConverterTestBase {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final MercifulJsonConverter CONVERTER = new MercifulJsonConverter(true,"__");

  @Test
  public void basicConversion() throws IOException {
    Schema simpleSchema = SchemaTestUtil.getSimpleSchema();
    String name = "John Smith";
    int number = 1337;
    String color = "Blue. No yellow!";
    Map<String, Object> data = new HashMap<>();
    data.put("name", name);
    data.put("favorite_number", number);
    data.put("favorite_color", color);
    String json = MAPPER.writeValueAsString(data);

    GenericRecord rec = new GenericData.Record(simpleSchema);
    rec.put("name", name);
    rec.put("favorite_number", number);
    rec.put("favorite_color", color);

    assertEquals(rec, CONVERTER.convert(json, simpleSchema));
  }

  @ParameterizedTest
  @MethodSource("dataNestedJsonAsString")
  void nestedJsonAsString(String nameInput) throws IOException {
    Schema simpleSchema = SchemaTestUtil.getSimpleSchema();
    String json = String.format("{\"name\": %s, \"favorite_number\": 1337, \"favorite_color\": 10}", nameInput);

    GenericRecord rec = new GenericData.Record(simpleSchema);
    rec.put("name", nameInput);
    rec.put("favorite_number", 1337);
    rec.put("favorite_color", "10");

    assertEquals(rec, CONVERTER.convert(json, simpleSchema));
  }

  /**
   * Covered case:
   * Avro Logical Type: Decimal
   * Exhaustive unsupported input coverage.
   */
  @ParameterizedTest
  @MethodSource("decimalBadCases")
  void decimalLogicalTypeInvalidCaseTest(String avroFile, String strInput, Double numInput,
                                         boolean testFixedByteArray) throws IOException {
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(avroFile);

    Map<String, Object> data = new HashMap<>();
    if (strInput != null) {
      data.put("decimalField", strInput);
    } else if (numInput != null) {
      data.put("decimalField", numInput);
    } else if (testFixedByteArray) {
      // Convert the fixed value to int array, which is used as json value literals.
      int[] intArray = {0, 0, 48, 57};
      data.put("decimalField", intArray);
    }
    String json = MAPPER.writeValueAsString(data);

    // Schedule with timestamp same as that of committed instant
    assertThrows(HoodieJsonToAvroConversionException.class, () -> {
      CONVERTER.convert(json, schema);
    });
  }

  /**
   * Covered case:
   * Avro Logical Type: Decimal
   * Avro type: bytes, fixed
   * Input: Check test parameter
   * Output: Object using Byte data type as the schema specified.
   * */
  @ParameterizedTest
  @MethodSource("decimalGoodCases")
  void decimalLogicalTypeTest(String avroFilePath, String groundTruth, String strInput,
                              Number numInput, boolean testFixedByteArray) throws IOException {
    BigDecimal bigDecimal = new BigDecimal(groundTruth);
    Map<String, Object> data = new HashMap<>();

    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(avroFilePath);
    GenericRecord record = new GenericData.Record(schema);
    Conversions.DecimalConversion conv = new Conversions.DecimalConversion();
    Schema decimalFieldSchema = schema.getField("decimalField").schema();

    // Decide the decimal field input according to the test dimension.
    if (strInput != null) {
      data.put("decimalField", strInput); // String number input
    } else if (numInput != null) {
      data.put("decimalField", numInput); // Number input
    } else if (testFixedByteArray) {
      // Fixed byte array input.
      // Example: 123.45 - byte array [0, 0, 48, 57].
      Schema fieldSchema = schema.getField("decimalField").schema();
      GenericFixed fixedValue = new Conversions.DecimalConversion().toFixed(
          bigDecimal, fieldSchema, fieldSchema.getLogicalType());
      // Convert the fixed value to int array, which is used as json value literals.
      byte[] byteArray = fixedValue.bytes();
      int[] intArray = new int[byteArray.length];
      for (int i = 0; i < byteArray.length; i++) {
        // Byte is signed in Java, int is 32-bit. Convert by & 0xFF to handle negative values correctly.
        intArray[i] = byteArray[i] & 0xFF;
      }
      data.put("decimalField", intArray);
    }

    // Decide the decimal field expected output according to the test dimension.
    if (avroFilePath.equals(DECIMAL_AVRO_FILE_PATH) || avroFilePath.equals(DECIMAL_ZERO_SCALE_AVRO_FILE_PATH)) {
      record.put("decimalField", conv.toBytes(bigDecimal, decimalFieldSchema, decimalFieldSchema.getLogicalType()));
    } else {
      record.put("decimalField", conv.toFixed(bigDecimal, decimalFieldSchema, decimalFieldSchema.getLogicalType()));
    }

    String json = MAPPER.writeValueAsString(data);

    GenericRecord real = CONVERTER.convert(json, schema);
    assertEquals(record, real);
  }

  // tests cases where decimals with fraction `.0` can be interpreted as having scale > 0
  @ParameterizedTest
  @MethodSource("zeroScaleDecimalCases")
  void zeroScaleDecimalConversion(String inputValue, String expected, boolean shouldConvert) {
    Schema schema = new Schema.Parser().parse("{\"namespace\": \"example.avro\",\"type\": \"record\",\"name\": \"decimalLogicalType\",\"fields\": [{\"name\": \"decimalField\", "
        + "\"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 0}}]}");
    String json = String.format("{\"decimalField\":%s}", inputValue);

    if (shouldConvert) {
      GenericRecord record = new GenericData.Record(schema);
      Conversions.DecimalConversion conv = new Conversions.DecimalConversion();
      Schema decimalFieldSchema = schema.getField("decimalField").schema();
      record.put("decimalField", conv.toBytes(new BigDecimal(expected), decimalFieldSchema, decimalFieldSchema.getLogicalType()));

      GenericRecord real = CONVERTER.convert(json, schema);
      assertEquals(record, real);
    } else {
      assertThrows(HoodieJsonToAvroConversionException.class, () -> CONVERTER.convert(json, schema));
    }
  }

  /**
   * Covered case:
   * Avro Logical Type: Duration
   * Avro type: 12 byte fixed
   * Input: 3-element list [month, days, milliseconds]
   * Output: Object using the avro data type as the schema specified.
   * */
  @ParameterizedTest
  @MethodSource("durationGoodCases")
  void durationLogicalTypeTest(int months, int days, int milliseconds) throws IOException {
    List<Integer> num = new ArrayList<>();
    num.add(months);
    num.add(days);
    num.add(milliseconds);

    Map<String, Object> data = new HashMap<>();
    data.put("duration", num);
    String json = MAPPER.writeValueAsString(data);

    ByteBuffer buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(months);  // months
    buffer.putInt(days); // days
    buffer.putInt(milliseconds); // milliseconds
    buffer.flip();
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(DURATION_AVRO_FILE_PATH);
    GenericRecord durationRecord = new GenericData.Record(schema);
    durationRecord.put("duration", new GenericData.Fixed(schema.getField("duration").schema(), buffer.array()));

    GenericRecord real = CONVERTER.convert(json, schema);
    assertEquals(durationRecord, real);
  }

  @ParameterizedTest
  @MethodSource("durationBadCases")
  void durationLogicalTypeBadTest(String schemaFile, Object input) throws IOException {
    Map<String, Object> data = new HashMap<>();
    data.put("duration", input);
    String json = MAPPER.writeValueAsString(data);

    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(schemaFile);
    // Schedule with timestamp same as that of committed instant
    assertThrows(HoodieJsonToAvroConversionException.class, () -> {
      CONVERTER.convert(json, schema);
    });
  }


  /**
   * Covered case:
   * Avro Logical Type: Date
   * Avro type: int
   * Input: Check parameter definition
   * Output: Object using the avro data type as the schema specified.
   * */
  @ParameterizedTest
  @MethodSource("dateGoodCaseProvider")
  void dateLogicalTypeTest(int groundTruth, Object dateInput) throws IOException {
    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(DATE_AVRO_FILE_PATH);
    GenericRecord record = new GenericData.Record(schema);
    record.put("dateField", groundTruth);

    Map<String, Object> data = new HashMap<>();
    data.put("dateField", dateInput);
    String json = MAPPER.writeValueAsString(data);
    GenericRecord real = CONVERTER.convert(json, schema);
    assertEquals(record, real);
  }

  /**
   * Covered case:
   * Avro Logical Type: Date
   * Invalid schema configuration.
   * */
  @ParameterizedTest
  @MethodSource("dateBadCaseProvider")
  void dateLogicalTypeTest(
      String schemaFile, Object input) throws IOException {
    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(schemaFile);

    Map<String, Object> data = new HashMap<>();
    data.put("dateField", input);
    String json = MAPPER.writeValueAsString(data);
    assertThrows(HoodieJsonToAvroConversionException.class, () -> {
      CONVERTER.convert(json, schema);
    });
  }

  private static final String LOCAL_TIME_AVRO_FILE_PATH = "/local-timestamp-logical-type.avsc";
  /**
   * Covered case:
   * Avro Logical Type: localTimestampMillisField & localTimestampMillisField
   * Avro type: long for both
   * Input: Check parameter definition
   * Output: Object using the avro data type as the schema specified.
   * */
  @ParameterizedTest
  @MethodSource("localTimestampGoodCaseProvider")
  void localTimestampLogicalTypeGoodCaseTest(
      Long expectedMicroSecOfDay, Object timeMilli, Object timeMicro) throws IOException {
    // Example inputs
    long microSecOfDay = expectedMicroSecOfDay;
    long milliSecOfDay = expectedMicroSecOfDay / 1000; // Represents 12h 30 min since the start of the day

    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(LOCAL_TIME_AVRO_FILE_PATH);
    GenericRecord record = new GenericData.Record(schema);
    record.put("localTimestampMillisField", milliSecOfDay);
    record.put("localTimestampMicrosField", microSecOfDay);

    Map<String, Object> data = new HashMap<>();
    data.put("localTimestampMillisField", timeMilli);
    data.put("localTimestampMicrosField", timeMicro);
    String json = MAPPER.writeValueAsString(data);
    GenericRecord real = CONVERTER.convert(json, schema);
    assertEquals(record, real);
  }

  @ParameterizedTest
  @MethodSource("localTimestampBadCaseProvider")
  void localTimestampLogicalTypeBadTest(
      String schemaFile, Object input) throws IOException {
    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(schemaFile);
    Map<String, Object> data = new HashMap<>();
    data.put("timestamp", input);
    String json = MAPPER.writeValueAsString(data);
    // Schedule with timestamp same as that of committed instant
    assertThrows(HoodieJsonToAvroConversionException.class, () -> {
      CONVERTER.convert(json, schema);
    });
  }

  /**
   * Covered case:
   * Avro Logical Type: localTimestampMillisField & localTimestampMillisField
   * Avro type: long for both
   * Input: Check parameter definition
   * Output: Object using the avro data type as the schema specified.
   * */
  @ParameterizedTest
  @MethodSource("timestampGoodCaseProvider")
  void timestampLogicalTypeGoodCaseTest(
      Long expectedMicroSecOfDay, Object timeMilli, Object timeMicro) throws IOException {
    // Example inputs
    long microSecOfDay = expectedMicroSecOfDay;
    long milliSecOfDay = expectedMicroSecOfDay / 1000; // Represents 12h 30 min since the start of the day

    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(TIMESTAMP_AVRO_FILE_PATH);
    GenericRecord record = new GenericData.Record(schema);
    record.put("timestampMillisField", milliSecOfDay);
    record.put("timestampMicrosField", microSecOfDay);

    Map<String, Object> data = new HashMap<>();
    data.put("timestampMillisField", timeMilli);
    data.put("timestampMicrosField", timeMicro);
    String json = MAPPER.writeValueAsString(data);
    GenericRecord real = CONVERTER.convert(json, schema);
    assertEquals(record, real);
  }

  @ParameterizedTest
  @MethodSource("timestampBadCaseProvider")
  void timestampLogicalTypeBadTest(Object badInput) throws IOException {
    // Define the schema for the date logical type
    String validInput = "2024-05-13T23:53:36.000Z";

    // Only give one of the fields invalid value so that both field processor can have branch coverage.
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(TIMESTAMP_AVRO_FILE_PATH);
    Map<String, Object> data = new HashMap<>();
    data.put("timestampMillisField", validInput);
    data.put("timestampMicrosField", badInput);
    // Schedule with timestamp same as that of committed instant
    assertThrows(HoodieJsonToAvroConversionException.class, () -> {
      CONVERTER.convert(MAPPER.writeValueAsString(data), schema);
    });

    data.clear();
    data.put("timestampMillisField", badInput);
    data.put("timestampMicrosField", validInput);
    // Schedule with timestamp same as that of committed instant
    assertThrows(HoodieJsonToAvroConversionException.class, () -> {
      CONVERTER.convert(MAPPER.writeValueAsString(data), schema);
    });
  }

  private static final String TIME_AVRO_FILE_PATH = "/time-logical-type.avsc";
  /**
   * Covered case:
   * Avro Logical Type: time-micros & time-millis
   * Avro type: long for time-micros, int for time-millis
   * Input: Check parameter definition
   * Output: Object using the avro data type as the schema specified.
   * */
  @ParameterizedTest
  @MethodSource("timeGoodCaseProvider")
  void timeLogicalTypeTest(Long expectedMicroSecOfDay, Object timeMilli, Object timeMicro) throws IOException {
    // Example inputs
    long microSecOfDay = expectedMicroSecOfDay;
    int milliSecOfDay = (int) (expectedMicroSecOfDay / 1000); // Represents 12h 30 min since the start of the day

    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(TIME_AVRO_FILE_PATH);
    GenericRecord record = new GenericData.Record(schema);
    record.put("timeMicroField", microSecOfDay);
    record.put("timeMillisField", milliSecOfDay);

    Map<String, Object> data = new HashMap<>();
    data.put("timeMicroField", timeMicro);
    data.put("timeMillisField", timeMilli);
    String json = MAPPER.writeValueAsString(data);
    GenericRecord real = CONVERTER.convert(json, schema);
    assertEquals(record, real);
  }

  @ParameterizedTest
  @MethodSource("timeBadCaseProvider")
  void timeLogicalTypeBadCaseTest(Object invalidInput) throws IOException {
    String validInput = "00:00:00";
    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(TIME_AVRO_FILE_PATH);

    // Only give one of the field invalid value at a time so that both processor type can have coverage.
    Map<String, Object> data = new HashMap<>();
    data.put("timeMicroField", validInput);
    data.put("timeMillisField", invalidInput);
    // Schedule with timestamp same as that of committed instant
    assertThrows(HoodieJsonToAvroConversionException.class, () -> {
      CONVERTER.convert(MAPPER.writeValueAsString(data), schema);
    });

    data.clear();
    data.put("timeMicroField", invalidInput);
    data.put("timeMillisField", validInput);
    // Schedule with timestamp same as that of committed instant
    assertThrows(HoodieJsonToAvroConversionException.class, () -> {
      CONVERTER.convert(MAPPER.writeValueAsString(data), schema);
    });
  }

  private static final String UUID_AVRO_FILE_PATH = "/uuid-logical-type.avsc";
  /**
   * Covered case:
   * Avro Logical Type: uuid
   * Avro type: string
   * Input: uuid string
   * Output: Object using the avro data type as the schema specified.
   * */
  @ParameterizedTest
  @MethodSource("uuidDimension")
  void uuidLogicalTypeTest(String uuid) throws IOException {
    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(UUID_AVRO_FILE_PATH);
    GenericRecord record = new GenericData.Record(schema);
    record.put("uuidField", uuid);

    Map<String, Object> data = new HashMap<>();
    data.put("uuidField", uuid);
    String json = MAPPER.writeValueAsString(data);
    GenericRecord real = CONVERTER.convert(json, schema);
    assertEquals(record, real);
  }

  @ParameterizedTest
  @MethodSource("nestedRecord")
  void nestedRecordTest(String contactInput, boolean isString) {
    String nestedSchemaStr =
        "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},"
            + "{\"name\":\"contact\",\"type\":{\"type\":\"record\",\"name\":\"Contact\","
            + "\"fields\":[{\"name\":\"email\",\"type\":\"string\"}]}}]}";
    String json = isString
        ? String.format("{\"name\":\"Jane Smith\",\"contact\":{\"email\":\"%s\"}}", contactInput)
        : String.format("{\"name\":\"Jane Smith\",\"contact\":{\"email\":%s}}", contactInput);
    Schema nestedSchema = new Schema.Parser().parse(nestedSchemaStr);
    GenericRecord userRecord = new GenericData.Record(nestedSchema);

    // Create the nested record for Contact
    Schema contactSchema = nestedSchema.getField("contact").schema();
    GenericRecord contactRecord = new GenericData.Record(contactSchema);

    // Set the email field in the nested Contact record
    contactRecord.put("email", contactInput);

    // Set the fields in the outer User record
    userRecord.put("name", "Jane Smith");
    userRecord.put("contact", contactRecord);

    assertEquals(userRecord, CONVERTER.convert(json, nestedSchema));
  }

  @Test
  public void conversionWithFieldNameSanitization() throws IOException {
    String sanitizedSchemaString = "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"__name\", \"type\": \"string\"}, "
        + "{\"name\": \"favorite__number\", \"type\": \"int\"}, {\"name\": \"favorite__color__\", \"type\": \"string\"}]}";
    Schema sanitizedSchema = Schema.parse(sanitizedSchemaString);
    String name = "John Smith";
    int number = 1337;
    String color = "Blue. No yellow!";
    Map<String, Object> data = new HashMap<>();
    data.put("$name", name);
    data.put("favorite-number", number);
    data.put("favorite.color!", color);
    String json = MAPPER.writeValueAsString(data);

    GenericRecord rec = new GenericData.Record(sanitizedSchema);
    rec.put("__name", name);
    rec.put("favorite__number", number);
    rec.put("favorite__color__", color);

    assertEquals(rec, CONVERTER.convert(json, sanitizedSchema));
  }

  @Test
  public void conversionWithFieldNameAliases() throws IOException {
    String schemaStringWithAliases = "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\", \"aliases\": [\"$name\"]}, "
        + "{\"name\": \"favorite_number\",  \"type\": \"int\", \"aliases\": [\"unused\", \"favorite-number\"]}, {\"name\": \"favorite_color\", \"type\": \"string\", \"aliases\": "
        + "[\"favorite.color!\"]}, {\"name\": \"unmatched\", \"type\": \"string\", \"default\": \"default_value\"}]}";
    Schema sanitizedSchema = Schema.parse(schemaStringWithAliases);
    String name = "John Smith";
    int number = 1337;
    String color = "Blue. No yellow!";
    Map<String, Object> data = new HashMap<>();
    data.put("$name", name);
    data.put("favorite-number", number);
    data.put("favorite.color!", color);
    String json = MAPPER.writeValueAsString(data);

    GenericRecord rec = new GenericData.Record(sanitizedSchema);
    rec.put("name", name);
    rec.put("favorite_number", number);
    rec.put("favorite_color", color);

    assertEquals(rec, CONVERTER.convert(json, sanitizedSchema));
  }

  @ParameterizedTest
  @MethodSource("encodedDecimalScalePrecisionProvider")
  void testEncodedDecimal(int scale, int precision) throws JsonProcessingException {
    Random rand = new Random();
    BigDecimal decfield = BigDecimal.valueOf(rand.nextDouble())
        .setScale(scale, RoundingMode.HALF_UP).round(new MathContext(precision, RoundingMode.HALF_UP));
    Map<String, Object> data = new HashMap<>();
    data.put("_row_key", "mykey");
    long timestamp = 214523432;
    data.put("timestamp", timestamp);
    data.put("rider", "myrider");
    data.put("decfield", Base64.getEncoder().encodeToString(decfield.unscaledValue().toByteArray()));
    data.put("driver", "mydriver");
    data.put("fare", rand.nextDouble() * 100);
    data.put("_hoodie_is_deleted", false);
    String json = MAPPER.writeValueAsString(data);
    Schema tripSchema = new Schema.Parser().parse(
        TRIP_ENCODED_DECIMAL_SCHEMA.replace("6", Integer.toString(scale)).replace("10", Integer.toString(precision)));
    GenericRecord genrec = CONVERTER.convert(json, tripSchema);
    Schema decimalFieldSchema = tripSchema.getField("decfield").schema();
    BigDecimal decoded = HoodieAvroUtils.convertBytesToBigDecimal(((ByteBuffer) genrec.get("decfield")).array(),
        (LogicalTypes.Decimal) decimalFieldSchema.getLogicalType());
    assertEquals(decfield, decoded);
  }

  @ParameterizedTest
  @MethodSource("encodedDecimalFixedScalePrecisionProvider")
  void testEncodedDecimalAvroSparkPostProcessorCase(int size, int scale, int precision) throws JsonProcessingException {
    Random rand = new Random();
    String postProcessSchemaString = String.format("{\"type\":\"record\",\"name\":\"tripUberRec\","
        + "\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"\"},{\"name\":\"_row_key\","
        + "\"type\":\"string\",\"doc\":\"\"},{\"name\":\"rider\",\"type\":\"string\",\"doc\":\"\"},"
        + "{\"name\":\"decfield\",\"type\":{\"type\":\"fixed\",\"name\":\"fixed\","
        + "\"namespace\":\"tripUberRec.decfield\",\"size\":%d,\"logicalType\":\"decimal\","
        + "\"precision\":%d,\"scale\":%d},\"doc\":\"\"},{\"name\":\"driver\",\"type\":\"string\","
        + "\"doc\":\"\"},{\"name\":\"fare\",\"type\":\"double\",\"doc\":\"\"},{\"name\":\"_hoodie_is_deleted\","
        + "\"type\":\"boolean\",\"doc\":\"\"}]}", size, precision, scale);
    Schema postProcessSchema = new Schema.Parser().parse(postProcessSchemaString);
    BigDecimal decfield = BigDecimal.valueOf(rand.nextDouble())
        .setScale(scale, RoundingMode.HALF_UP).round(new MathContext(precision, RoundingMode.HALF_UP));
    Map<String, Object> data = new HashMap<>();
    data.put("_row_key", "mykey");
    long timestamp = 214523432;
    data.put("timestamp", timestamp);
    data.put("rider", "myrider");
    data.put("decfield", Base64.getEncoder().encodeToString(decfield.unscaledValue().toByteArray()));
    data.put("driver", "mydriver");
    data.put("fare", rand.nextDouble() * 100);
    data.put("_hoodie_is_deleted", false);
    String json = MAPPER.writeValueAsString(data);
    GenericRecord genrec = CONVERTER.convert(json, postProcessSchema);
    GenericData.Fixed fixed = (GenericData.Fixed) genrec.get("decfield");
    Conversions.DecimalConversion decimalConverter = new Conversions.DecimalConversion();
    Schema decimalFieldSchema = postProcessSchema.getField("decfield").schema();
    assertEquals(decfield, decimalConverter.fromFixed(fixed, decimalFieldSchema, decimalFieldSchema.getLogicalType()));
  }
}