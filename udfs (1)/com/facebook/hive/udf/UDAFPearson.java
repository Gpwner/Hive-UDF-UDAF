package com.facebook.hive.udf;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * This is a simple UDAF that calculates Pearson correlation of two varibles.
 *
 */
public final class UDAFPearson extends UDAF {

    /**
     * Note that this is only needed if the internal state cannot be represented
     * by a primitive.
     *
     * The internal state can also contains fields with types like
     * ArrayList<String> and HashMap<String,Double> if needed.
     */
    public static class UDAFPearsonState {
	private double sum_sq_x;
	private double sum_sq_y;
	private double sum_co_product;;
	private double mean_x;
	private double mean_y;
	private double N;
    }

    /**
     * The actual class for doing the aggregation. Hive will automatically look
     * for all internal classes of the UDAF that implements UDAFEvaluator.
     */
    public static class UDAFPearsonEvaluator implements UDAFEvaluator {

	UDAFPearsonState state;

	public UDAFPearsonEvaluator() {
	    super();
	    state = new UDAFPearsonState();
	    init();
	}

	/**
	 * Reset the state of the aggregation.
	 */
	public void init() {
	    state.sum_sq_x = 0.0;
	    state.sum_sq_y = 0.0;
	    state.sum_co_product = 0.0;
	    state.mean_x = 0.0;
	    state.mean_y = 0.0;
	    state.N = 0.0;
	}

	/**
	 * Iterate through one row of original data.
	 *
	 * The number and type of arguments need to the same as we call this UDAF
	 * from Hive command line.
	 *
	 * This function should always return true.
	 */
	public boolean iterate(Double y, Double x) {
	    if (y != null && x != null) {
		state.N++;
		double delta_x = x - state.mean_x;
		double delta_y = y - state.mean_y;
		state.mean_x += delta_x / state.N;
		state.mean_y += delta_y / state.N;

		state.sum_sq_x += delta_x * (x - state.mean_x);
		state.sum_sq_y += delta_y * (y - state.mean_y);

		state.sum_co_product += delta_x * delta_y; // ????
	    }
	    return true;
	}

	/**
	 * Terminate a partial aggregation and return the state. If the state is a
	 * primitive, just return primitive Java classes like Integer or String.
	 */
	public UDAFPearsonState terminatePartial() {
	    // Return null if we have no data.
	    return state.N == 0 ? null : state;
	}

	/**
	 * Merge with a partial aggregation.
	 *
	 * This function should always have a single argument which has the same
	 * type as the return value of terminatePartial().
	 */
	public boolean merge(UDAFPearsonState o) {
	    if (o != null) {
		double delta_x = state.mean_x - o.mean_x;
		state.mean_x = o.mean_x + delta_x * state.N / (state.N + o.N);

		double delta_y = state.mean_y - o.mean_y;
		state.mean_y = o.mean_y + delta_y * state.N / (state.N + o.N);

		state.sum_sq_x += o.sum_sq_x + delta_x * delta_x * state.N * o.N / (state.N + o.N);
		state.sum_sq_y += o.sum_sq_y + delta_y * delta_y * state.N * o.N / (state.N + o.N);

		state.sum_co_product = 0.0;  /// ??????
		state.N += o.N;
	    }
	    return true;
	}

	/**
	 * Terminates the aggregation and return the final result.
	 */
	public Double terminate() {
	    if (state.N == 0) {
		return null;
	    }
	    double cov_x_y = state.sum_co_product / state.N;
	    double sd_x = Math.sqrt(state.sum_sq_x / state.N);
	    double sd_y = Math.sqrt(state.sum_sq_y / state.N);
	    return Double.valueOf(cov_x_y / sd_x / sd_y);
	}
    }

    private UDAFPearson() {
	// prevent instantiation
    }
}
