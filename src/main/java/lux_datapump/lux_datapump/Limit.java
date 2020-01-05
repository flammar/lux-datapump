package lux_datapump.lux_datapump;


public class Limit {
	public static enum Type {
		LOWER(true, ">"), UPPER(false, "<");

		public final boolean including;
		public final String expression;

		Type(final boolean including, final String expression) {
			this.including = including;
			this.expression = expression;

		}
	}

	public final Limit.Type type;
	public final Object value;
	public final boolean including;

	public Limit(final Limit.Type type, final Object value, final boolean including) {
		this.type = type;
		this.value = value;
		this.including = including;
	}

	public Limit(final Limit.Type type, final Object value) {
		this(type, value, type.including);
	}

	public String conditionalExpression() {
		if (value == null)
			return null;
		return String.join(" ", "", type.expression + (including ? "=" : ""), "?", "");
	}
}