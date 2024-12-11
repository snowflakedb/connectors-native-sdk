package com.snowflake.connectors.util.variant;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.snowpark_java.types.Variant;
import java.time.LocalDate;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class VariantMapperTest {

  private static final ZoneId ZONE_ID = ZoneId.of("Europe/Berlin");

  static class ClassWithDate {
    private LocalDate localDate;
    private ZoneId zoneId;

    public ClassWithDate() {}

    public ClassWithDate(LocalDate localDate, ZoneId zoneId) {
      this.localDate = localDate;
      this.zoneId = zoneId;
    }

    public LocalDate getLocalDate() {
      return localDate;
    }

    public ZoneId getZoneId() {
      return zoneId;
    }
  }

  @Test
  void shouldSerializeClassWithDate() {
    // given
    LocalDate now = LocalDate.now();
    ClassWithDate classWithDate = new ClassWithDate(now, ZONE_ID);

    // when
    Variant mapped = VariantMapper.mapToVariant(classWithDate);

    // then
    assertThat(mapped.asMap().get("localDate").asString()).isEqualTo(now.toString());
    assertThat(mapped.asMap().get("zoneId").asString()).isEqualTo(ZONE_ID.toString());
  }

  @Test
  void shouldDeserializeClassWithDate() {
    // given
    String date = "2020-01-01";
    Variant classWithDateVariant =
        new Variant("{\"localDate\":\"" + date + "\", \"zoneId\": \"" + ZONE_ID + "\"}");

    // when
    ClassWithDate classWithDate =
        VariantMapper.mapVariant(classWithDateVariant, ClassWithDate.class);

    // then
    assertThat(classWithDate.getLocalDate()).isEqualTo(LocalDate.parse(date));
    assertThat(classWithDate.getZoneId()).isEqualTo(ZONE_ID);
  }
}
