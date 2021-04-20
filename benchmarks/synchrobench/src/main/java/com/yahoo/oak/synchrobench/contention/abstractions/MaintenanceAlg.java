/*
 * Copyright 2020, Verizon Media.
 * Licensed under the terms of the Apache 2.0 license.
 * Please see LICENSE file in the project root for terms.
 */

package com.yahoo.oak.synchrobench.contention.abstractions;

/**
 * Compositional map interface
 *
 * @author Vincent Gramoli
 */
public interface MaintenanceAlg {

    boolean stopMaintenance();

    long getStructMods();

    int numNodes();
}
