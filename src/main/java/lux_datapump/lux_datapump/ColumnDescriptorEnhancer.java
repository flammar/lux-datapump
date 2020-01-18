package lux_datapump.lux_datapump;

public interface ColumnDescriptorEnhancer {
	default ColumnDescriptor enhance(ColumnDescriptor d) {
		return d;
	};
}
