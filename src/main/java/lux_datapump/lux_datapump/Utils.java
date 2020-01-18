package lux_datapump.lux_datapump;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

public class Utils {

	@FunctionalInterface
	public static interface ThrowingConsumer<T> extends Consumer<T> {
		@Override
		default void accept(final T elem) {
			try {
				throwingAccept(elem);
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		}

		void throwingAccept(T elem) throws Exception;
	}

	@FunctionalInterface
	public static interface ThrowingFunction<T, R> extends Function<T, R> {
		@Override
		default R apply(final T elem) {
			try {
				return applyThrows(elem);
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		}

		R applyThrows(T elem) throws Exception;
	}

	private Utils() {
	}

	public static Iterator<List<ValueDescriptor>> toIterator(ResultSet src, List<ColumnDescriptor> descs) {
		return new Iterator<List<ValueDescriptor>>() {
			private boolean wasNext = false, wentOut = false;
			private List<ValueDescriptor> current = null;

			@Override
			public boolean hasNext() {
				return tryNext(src);
			}

			@Override
			public List<ValueDescriptor> next() {
				if (!hasNext())
					return null;
				return current;
			}

			private boolean tryNext(ResultSet src) {
				if (!wentOut && !wasNext) {
					try {
						wentOut = !src.next();
						current = wentOut ? null : new ArrayList<>(rowToList());
						wasNext = true;
					} catch (SQLException e) {
						wentOut = true;
						current = null;
					}
				}
				return !wentOut;
			}

			private List<ValueDescriptor> rowToList() {
				return descs.stream().map(cd -> {
					try {
						return new ValueDescriptor(cd.getter.perform(src, cd.srcIndex), cd);
					} catch (SQLException e) {
						e.printStackTrace();
						return null;
					}
				}).collect(Collectors.toList());
			}
		};
	}

	public static <T> Iterator<Iterable<T>> portionize(Iterator<T> baseIterator, int portionSize) {
		return new Iterator<Iterable<T>>() {

			@Override
			public boolean hasNext() {
				return baseIterator.hasNext();
			}

			@Override
			public Iterable<T> next() {
				return new Iterable<T>() {
					private int i = portionSize;
					private final Iterator<T> base = baseIterator;

					@Override
					public Iterator<T> iterator() {
						return new Iterator<T>() {

							@Override
							public T next() {
								i--;
								return base.hasNext() ? base.next() : null;
							}

							@Override
							public boolean hasNext() {
								return i > 0 && base.hasNext();
							}
						};
					}
				};
			}
		};
	}

	public static <R> IntFunction<R> unbox(Function<Integer, R> base) {
		return new IntFunction<R>(){

			@Override
			public R apply(int value) {
				return base.apply(value);
			}};
	}

	public static <U, V> Function<U, V> wrap(ThrowingFunction<U, V> base) {
		return base;
	}

	public static <U, R, X extends Exception> Function<U, R> wrap(ThrowingFunction<U, R> base, BiFunction<U, X, R> xh) {
		return new ThrowingFunction<U, R>() {

			@SuppressWarnings("unchecked")
			@Override
			public R applyThrows(U elem) throws Exception {
				final U elem1 = elem;
				try {
					return base.applyThrows(elem1);
				} catch (final Exception e) {
					return xh.apply(elem, (X) e);
				}
			}

			@Override
			public R apply(U elem) {
				return ThrowingFunction.super.apply(elem);
			}
		};
	}


	public static <U> Consumer<U> wrap(ThrowingConsumer<U> base) {
		return base;
	}

	public static <U, X extends Exception> Consumer<U> wrap(ThrowingConsumer<U> base, BiConsumer<U, X> xh) {
		return base;
	}

	public static <C> AbstractList<C> list (IntFunction<C> item, IntSupplier size){
		return new AbstractList<C>() {

			@Override
			public C get(int index) {
				return item.apply(index);
			}

			@Override
			public int size() {
				return size.getAsInt();
			}
		};
		
	}
	
	public static <C> AbstractList<C> list (IntFunction<C> item, int size){
		return list(item, ()->size);
	}
	
	public static <T> Iterator<T> counted(Iterator<T> iterator, Counter counter){
		return new Iterator<T>() {

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public T next() {
				counter.increment();
				return iterator.next();
			}
		};
	} 
}
