package lux_datapump.lux_datapump;

import static lux_datapump.lux_datapump.Utils.list;
import static lux_datapump.lux_datapump.Utils.unbox;
import static lux_datapump.lux_datapump.Utils.wrap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lux_datapump.lux_datapump.Limit.Type;
import lux_datapump.lux_datapump.Utils.ThrowingFunction;

/*
 * Core class. To be configured from outside, via DI container or somehow else. To be used via external starter.
 * 
 * @author mskidan
 *
 */
public class Performer {
	private Integer portionSize;
	private String srcTableName;
	private String dstTableName;
	/**
	 * [<lower> -> <upper>]
	 */
	private Map<String, Map.Entry<Limit, Limit>> limits;
	private String srcDataSource;
	private String dstDataSource;
//	dstColDescs;
	private ColumnDescriptorEnhancer columnDescriptorEnhancer = new ColumnDescriptorEnhancer() {
	};
	private boolean autocommit;
	private boolean predelete;

	public void perform() throws SQLException {

		try (final Connection dstConn = DriverManager.getConnection(dstDataSource);) {
			try (Connection srcConn = DriverManager.getConnection(srcDataSource);) {
				final List<ColumnDescriptor> dstColDescs = getColDescs(dstConn, dstTableName).stream()
						.map(columnDescriptorEnhancer::enhance).collect(Collectors.toList());
				// sorted - for case of java.sql.ResultSet.TYPE_FORWARD_ONLY cursors
				final String srcColList = String.join(", ",
						dstColDescs.stream().sorted((cd0, cd1) -> cd0.srcIndex - cd1.srcIndex).map(d -> d.srcExpr)
								.collect(Collectors.toList()));
				String where = whereCond();
				if (autocommit)
					dstConn.setAutoCommit(true);
				if (predelete)
					dstConn.createStatement().executeUpdate(String.format("DELETE FROM {0}{1}", dstTableName, where));
				try (Statement srcStatement = srcConn.createStatement();
						ResultSet src = srcStatement.executeQuery(
								MessageFormat.format("SELECT {1} FROM {0}{2}", srcTableName, srcColList, where));) {
					Iterator<List<ValueDescriptor>> baseIterator = Utils.toIterator(src, dstColDescs);
					final String dstColList = String.join(", ",
							dstColDescs.stream().map(d -> d.dstName).collect(Collectors.toList()));
					final List<String> dstValList = dstColDescs.stream().map(d -> d.dstExpr)
							.collect(Collectors.toList());
					final String dstQuery = MessageFormat.format("INSERT INTO {0} ( {1} ) VALUES ( {2} )", dstTableName,
							dstColList, String.join(", ", dstValList));
					Utils.portionize(baseIterator, this.portionSize).forEachRemaining(wrap(data -> {
						perform(dstConn, dstQuery, data);
					}));

				} finally {
				}
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

	private List<ColumnDescriptor> getColDescs(final Connection dstConn, final String tableName) throws SQLException {
		try (Statement preStt = dstConn.createStatement();
				ResultSet executeQuery = preStt
						.executeQuery(MessageFormat.format("SELECT * FROM {0} WHERE 0 = 1", tableName));) {
			final ResultSetMetaData metaData = executeQuery.getMetaData();
			final ArrayList<ColumnDescriptor> arrayList = new ArrayList<ColumnDescriptor>(
					createColumnDescriptors(metaData));
			return arrayList;
		} finally {
		}
	}

	private List<ColumnDescriptor> createColumnDescriptors(final ResultSetMetaData metaData) throws SQLException {
		return list(
				unbox(wrap((ThrowingFunction<Integer, ColumnDescriptor>) ((i) -> createColumnDescriptor(metaData, i)))),
				metaData.getColumnCount());
	}

	private static ColumnDescriptor createColumnDescriptor(final ResultSetMetaData metaData, Integer i)
			throws SQLException {
		// should be replaced/enhanced
		return new ColumnDescriptor(i + 1, metaData.getColumnName(i + 1), metaData.getColumnType(i + 1));
	}

	public List<Integer> perform(final Connection connection/* = getDatabaseConnection(); */, final String query,
			final Iterable<List<ValueDescriptor>> data) throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			data.forEach(wrap(vds -> {
				vds.forEach(wrap((ValueDescriptor vd) -> {
					vd.descriptor.setter.perform(statement, vd.descriptor.dstIndex, vd.value, vd.descriptor.sqlType);
				}));
				statement.addBatch();
			}));
			final int[] executeBatch = statement.executeBatch();
			return list(index -> Integer.valueOf(executeBatch[index]), executeBatch.length);
		} finally {

		}

	}

}
