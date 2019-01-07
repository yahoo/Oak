package com.oath.oak.synchrobench.contention.abstractions;

import java.util.Collection;

/*
 * Compositional integer set interface
 * 
 * @author Vincent Gramoli
 *
 */
public interface CompositionalIntSet {
	
	void fill(int range, long size);
	
	boolean addInt(int x);
	boolean removeInt(int x);
	boolean containsInt(int x);
	Object getInt(int x);

	boolean addAll(Collection<Integer> c);
	boolean removeAll(Collection<Integer> c);
	
	int size();
	
	void clear();
	
	String toString();
	
	Object putIfAbsent(int x, int y);
	
	
}
