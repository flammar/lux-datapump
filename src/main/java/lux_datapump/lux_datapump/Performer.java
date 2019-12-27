package lux_datapump.lux_datapump;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lux_datapump.lux_datapump.Performer.Limit.Type;

public class Performer {
	public static class Limit {
		public static enum Type {
			LOWER(true, ">"), UPPER(false, "<");

			public final boolean including;
			public final String expression;

			Type(final boolean including, final String expression) {
				this.including = including;
				this.expression = expression;

			}
		}

		public final Type type;
		public final Object value;
		public final boolean including;

		public Limit(final Type type, final Object value, final boolean including) {
			this.type = type;
			this.value = value;
			this.including = including;
		}

		public Limit(final Type type, final Object value) {
			this(type, value, type.including);
		}

		public String conditionalExpression() {
			if (value == null)
				return null;
			return String.join(" ", "", type.expression + (including ? "=" : ""), "?", "");
		}
	}

	private Integer portionSize;
	private String srcTableName;
	private String dstTableName;
	private Map<String, Map.Entry<Limit, Limit>> limits;
	private String srcDataSource;
	private String dstDataSource;
	private List<ColumnDescriptor> dstColDescs;
	private boolean autocommit;
	private boolean predelete;

	public static class ColumnDescriptor {
		public final String name;
		public final int sqlType;

		public ColumnDescriptor(final String name, final int sqlType) {
			super();
			this.name = name;
			this.sqlType = sqlType;
		}
	}

	public void perform() throws SQLException {
		String where = whereCond();

		try (final Connection dstConn = DriverManager.getConnection(dstDataSource);) {
			if (autocommit)
				dstConn.setAutoCommit(true);
			if (predelete)
				dstConn.createStatement().executeUpdate(String.format("DELETE FROM {0}{1}", dstTableName, where));
			dstColDescs = getColDescs(dstConn, dstTableName);
			final String colList = String.join(", ",
					dstColDescs.stream().map(d -> d.name).collect(Collectors.toList()));
			final String dstQuery = MessageFormat.format("INSERT INTO {0} ( {1} ) VALUES ( {2} )", dstTableName,
					colList, String.join(", ", Collections.nCopies(dstColDescs.size(), "?")));
			try (Connection srcConn = DriverManager.getConnection(srcDataSource);
					Statement srcStatement = srcConn.createStatement();
					ResultSet src = srcStatement.executeQuery(
							MessageFormat.format("SELECT {1} FROM {0}{2}", srcTableName, colList, where));) {
				final boolean wasNext[] = new boolean[] { true };
				do {
					List<List<Object>> collectData = collectData(src, wasNext);
					perform(dstConn, dstQuery, collectData);
				} while (wasNext[0]);

			} finally {

			}
		} finally {
		}
	}

	private String whereCond() {
		final String cond = limits.entrySet().stream()
				.flatMap(e -> Stream.of(new Limit(Type.LOWER, e.getKey()), new Limit(Type.UPPER, e.getValue())))
				.filter(l -> l.value != null).map(Objects::toString).collect(Collectors.joining(" AND "));
		return cond.length() > 0 ? " WHERE " + cond : "";
	}

	private List<List<Object>> collectData(final ResultSet src, final boolean[] wasNext) throws SQLException {
		final List<List<Object>> data = new ArrayList<>();
		for (int i = portionSize; (wasNext[0] = src.next()) && i-- > 0;) {
			data.add(new ArrayList<>(new AbstractList<Object>() {

				private final int size = dstColDescs.size();

				@Override
				public Object get(final int index) {
					try {
						return src.getObject(index + 1);
					} catch (final SQLException e) {
						e.printStackTrace();
					}
					return null;
				}

				@Override
				public int size() {
					return size;
				}
			}));
		}
		return data;
	}

	private List<ColumnDescriptor> getColDescs(final Connection dstConn, final String tableName) throws SQLException {
//		Statement preStt = dstConn.createStatement();

		try (Statement preStt = dstConn.createStatement();
				ResultSet executeQuery = preStt
						.executeQuery(MessageFormat.format("SELECT * FROM {0} WHERE 0 = 1", tableName));) {
			final ResultSetMetaData metaData = executeQuery.getMetaData();
			final ArrayList<ColumnDescriptor> arrayList = new ArrayList<Performer.ColumnDescriptor>(
					new AbstractList<ColumnDescriptor>() {
						int columnCount = metaData.getColumnCount();

						@Override
						public ColumnDescriptor get(final int index) {
							try {
								return new ColumnDescriptor(metaData.getCatalogName(index + 1),
										metaData.getColumnType(index + 1));
							} catch (final SQLException e) {
							}
							return null;
						}

						@Override
						public int size() {
							return columnCount;
						}
					});
			return arrayList;
		} finally {
			// TODO: handle finally clause
		}
	}

	public List<Integer> perform(final Connection connection/* = getDatabaseConnection(); */, final String query,
			final List<List<Object>> data) throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			for (final List<Object> oo : data) {
				int i = 1;
				for (final Object o : oo) {
					i = setObject(statement, i++, o);
				}
				statement.addBatch();
			}
			;
			final int[] executeBatch = statement.executeBatch();
			return new AbstractList<Integer>() {

				@Override
				public Integer get(final int index) {
					return Integer.valueOf(executeBatch[index]);
				}

				@Override
				public int size() {
					return executeBatch.length;
				}
			};
		} finally {

		}

	}

	private int setObject(final PreparedStatement statement, final int i, final Object o) throws SQLException {
//		statement.setObject(i, o);
		statement.setObject(i, o, dstColDescs.get(i - 1).sqlType);

		return i;
	}

}
