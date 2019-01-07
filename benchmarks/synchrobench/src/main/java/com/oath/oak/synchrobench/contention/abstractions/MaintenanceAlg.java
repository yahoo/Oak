package com.oath.oak.synchrobench.contention.abstractions;

/*
 * Compositional map interface
 * 
 * @author Vincent Gramoli
 *
 */
public interface MaintenanceAlg {

	boolean stopMaintenance();
	long getStructMods();
	int numNodes();
}
