package com.oath.oak.synchrobench.contention.abstractions;

import java.util.Map;

/*
 * Compositional map interface
 * 
 * @author Vincent Gramoli
 *
 */
public interface CompositionalMap<K, V> extends Map<K, V> {

    class Vars {
        public long getCount = 0;
    	public long nodesTraversed = 0;
    	public long structMods = 0;
    }
    
    ThreadLocal<Vars> counts = new ThreadLocal<Vars>() {
        @Override
        protected synchronized Vars initialValue() {
            return new Vars();
        }
    };
	
	V putIfAbsent(K k, V v);

	void clear();
	
	int size();
}

