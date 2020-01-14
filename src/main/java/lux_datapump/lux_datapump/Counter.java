package lux_datapump.lux_datapump;

/**
 * @author ADMIN
 * 
 * single-threaded {@link #increment()} and {@link #set()}, multi-threaded {@link #get()}
 *
 */
public interface Counter {

	void set(long v);

	/**
	 * The most often operation.
	 */
	void increment();

	long get();

}
