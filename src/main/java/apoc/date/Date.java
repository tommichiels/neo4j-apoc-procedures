package apoc.date;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import apoc.Description;
import apoc.result.LongResult;
import apoc.result.MapResult;
import apoc.result.StringResult;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;


/**
 * @author tkroman
 * @since 9.04.2016
 */
public class Date {
	public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final int MILLIS_IN_SECOND = 1000;
	private static final String UTC_ZONE_ID = "UTC";
	private static final List<TemporalQuery<Consumer<Map<String, Object>>>> DT_FIELDS_SELECTORS = Arrays.asList(
			temporalQuery(ChronoField.YEAR),
			temporalQuery(ChronoField.MONTH_OF_YEAR),
			temporalQuery(ChronoField.DAY_OF_MONTH),
			temporalQuery(ChronoField.HOUR_OF_DAY),
			temporalQuery(ChronoField.MINUTE_OF_HOUR),
			temporalQuery(ChronoField.SECOND_OF_MINUTE),
			temporal -> map -> {
				Optional<ZoneId> zone = Optional.ofNullable(TemporalQueries.zone().queryFrom(temporal));
				zone.ifPresent(zoneId -> {
					String displayName = zoneId.getDisplayName(TextStyle.SHORT, Locale.ROOT);
					map.put("zoneid", displayName);
				});
			}
	);


	@Procedure
	@PerformsWrites
	@Description("CALL apoc.date.expire(node,time,'time-unit') - expire node in given time by setting :TTL label and `ttl` property")
	public void expire(@Name("node") Node node, @Name("time") long time, @Name("timeUnit") String timeUnit) {
		node.addLabel(Label.label("TTL"));
		node.setProperty("ttl",unit(timeUnit).toMillis(time));
	}

	@Procedure
	@Description("apoc.date.fieldsDefault('2012-12-23 13:10:50') - create structured map representation of date with entries for year,month,day,hour,minute,second,zoneid")
	public Stream<MapResult> fieldsDefault(final @Name("date") String date) {
		return fields(date, null);
	}

	@Procedure
	@Description("apoc.date.fields('2012-12-23','yyyy-MM-dd') - create structured map representation of date parsed with the given format with entries for year,month,day,hour,minute,second,zoneid")
	public Stream<MapResult> fields(final @Name("date") String date, final @Name("pattern") String pattern) {
		if (date == null) {
			return Stream.of(MapResult.empty());
		}
		DateTimeFormatter fmt = getDateTimeFormatter(pattern);
		TemporalAccessor temporal = fmt.parse(date);
		Map<String, Object> selectFields = new HashMap<>();

		for (final TemporalQuery<Consumer<Map<String, Object>>> query : DT_FIELDS_SELECTORS) {
			query.queryFrom(temporal).accept(selectFields);
		}

		return Stream.of(new MapResult(selectFields));
	}

	private TimeUnit unit(String unit) {
		if (unit == null) return TimeUnit.MILLISECONDS;

		switch (unit.toLowerCase()) {
			case "ms": case "milli":  case "millis": case "milliseconds": return TimeUnit.MILLISECONDS;
			case "s":  case "second": case "seconds": return TimeUnit.SECONDS;
			case "m":  case "minute": case "minutes": return TimeUnit.MINUTES;
			case "h":  case "hour":   case "hours":   return TimeUnit.HOURS;
			case "d":  case "day":    case "days":    return TimeUnit.DAYS;
//			case "month":case "months": return TimeUnit.MONTHS;
//			case "years":case "year": return TimeUnit.YEARS;
		}
		return TimeUnit.MILLISECONDS;
	}

	@Procedure
	@Description("apoc.date.formatDefault(12345,'ms|s|m|h|d') get string representation of time value in the specified unit using default format")
	public Stream<StringResult> formatDefault(final @Name("time") long time, @Name("unit") String unit) {
		return parse(unit(unit).toMillis(time), DEFAULT_FORMAT);
	}

	@Procedure
	@Description("apoc.date.format(12345,'ms|s|m|h|d','yyyy-MM-dd') get string representation of time value in the specified unit using specified format and UTC time zone")
	public Stream<StringResult> format(final @Name("time") long time, @Name("unit") String unit, @Name("format") String format) {
		return parse(unit(unit).toMillis(time), format, null);
	}

	@Procedure
	@Description("apoc.date.formatTimeZone(12345,'ms|s|m|h|d','yyyy-MM-dd HH:mm:ss zzz', 'ABC') get string representation of time value in the specified unit using specified format and specified time zone")
	public Stream<StringResult> formatTimeZone(final @Name("time") long time, @Name("unit") String unit, @Name("format") String format, @Name("timezone") String timezone) {
		return parse(unit(unit).toMillis(time), format, timezone);
	}

	@Procedure
	@Description("apoc.date.parseDefault('2012-12-23 13:10:50','ms|s|m|h|d') parse date string using the default format into the specified time unit")
	public Stream<LongResult> parseDefault(final @Name("time") String time, @Name("unit") String unit) {
		return parse(time, unit, DEFAULT_FORMAT);
	}

	@Procedure
	@Description("apoc.date.parse('2012-12-23','ms|s|m|h|d','yyyy-MM-dd') parse date string using the specified format into the specified time unit")
	public Stream<LongResult> parse(@Name("time") String time, @Name("unit") String unit, @Name("format") String format) {
		Long value = parseOrThrow(time, getFormat(format, null));
		Long valueInUnit = value == null ? null : unit(unit).convert(value, TimeUnit.MILLISECONDS);
		return Stream.of(new LongResult(valueInUnit));
	}

	public Stream<StringResult> parse(final @Name("millis") long millis, final @Name("pattern") String pattern) {
		return parse(millis, pattern, null);
	}

	private Stream<StringResult> parse(final @Name("millis") long millis, final @Name("pattern") String pattern, final @Name("timezone") String timezone) {
		if (millis < 0) {
			throw new IllegalArgumentException("The time argument should be >= 0, got: " + millis);
		}
		return Stream.of(new StringResult(getFormat(pattern, timezone).format(new java.util.Date(millis))));
	}

	private static DateFormat getFormat(final String pattern, final String timezone) {
		String actualPattern = getPattern(pattern);
		SimpleDateFormat format = new SimpleDateFormat(actualPattern);
		if (timezone != null) {
			format.setTimeZone(TimeZone.getTimeZone(timezone));
		} else if (!(containsTimeZonePattern(actualPattern))) {
			format.setTimeZone(TimeZone.getTimeZone(UTC_ZONE_ID));
		}
		return format;
	}

	private static DateTimeFormatter getDateTimeFormatter(final String pattern) {
		String actualPattern = getPattern(pattern);
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern(actualPattern);
		if (containsTimeZonePattern(actualPattern)) {
			return fmt;
		} else {
			return fmt.withZone(ZoneId.of(UTC_ZONE_ID));
		}
	}

	private static Long parseOrThrow(final String date, final DateFormat format) {
		if (date == null) return null;
		try {
			return format.parse(date).getTime();
		} catch (ParseException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private static boolean containsTimeZonePattern(final String pattern) {
		return pattern.matches("[XxZzVO]{1,3}");	// doesn't account for strings escaped with "'" (TODO?)
	}

	private static String getPattern(final String pattern) {
		return pattern == null ? DEFAULT_FORMAT : pattern;
	}

	private static TemporalQuery<Consumer<Map<String, Object>>> temporalQuery(final ChronoField field) {
		return temporal -> map -> {
			if (field.isSupportedBy(temporal)) {
				map.put(field.getBaseUnit().toString().toLowerCase(), field.getFrom(temporal));
			}
		};
	}
}
