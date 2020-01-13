package lux_datapump.lux_datapump;

public interface Counter {

	void set(long v);

	void inc();

	long get();

}
