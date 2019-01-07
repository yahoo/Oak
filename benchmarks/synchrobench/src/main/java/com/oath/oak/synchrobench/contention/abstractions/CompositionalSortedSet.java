package com.oath.oak.synchrobench.contention.abstractions;

import java.util.SortedSet;

public interface CompositionalSortedSet<E> extends SortedSet<E> {
	
	int size();

	String toString();
}

