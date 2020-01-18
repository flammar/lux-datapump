package lux_datapump.lux_datapump;

import lux_datapump.lux_datapump.ColumnDescriptor.Getter;
import lux_datapump.lux_datapump.ColumnDescriptor.Setter;

public class TsqlDatetime2ToOracleTimestampColumnDescriptorEnhancer implements ColumnDescriptorEnhancer {

	@Override
	public ColumnDescriptor enhance(ColumnDescriptor d) {
		return new ColumnDescriptor(d.srcExpr, d.sqlType, d.dstName, "TO_TIMESTAMP( ? )", Getter.GET_STRING,
				Setter.SET_OBJECT_OR_NULL, d.srcIndex, d.dstIndex);
	}

}
