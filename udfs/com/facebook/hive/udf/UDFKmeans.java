package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import java.util.ArrayList;

/**
 * Performs K-Means on a set of tuples.
 */
@Description(name = "udfkmeans",
             value = "_FUNC_(points, K, max_iterations) - Perform K-means clustering on a collection of tuples.  Returns an array of means.")

  public class UDFKmeans extends UDF {
    public int sample(double[] weights) throws SemanticException {
      double weight_sum = 0;
      for (int ii = 0; ii < weights.length; ++ii) {
        if (weights[ii] < 0.0) {
          return -3;
        }
        weight_sum += weights[ii];
      }

      double r = Math.random();

      if (weight_sum == 0.0) {
        return (int)(r * weights.length);
      }

      for (int ii = 0; ii < weights.length; ++ii) {
        if (r < weights[ii] / weight_sum) {
          return ii;
        }
        r -= weights[ii] / weight_sum;
      }

      return -2;
    }

    public double squared_dist(ArrayList<Double> center, ArrayList<Double> point) throws SemanticException {
      int M = point.size();
      if (M != center.size()) {
        throw new UDFArgumentTypeException(M,
                                           "This should never happen.");
      }

      double dist2 = 0;
      for (int mm = 0; mm < M; ++mm) {
        dist2 += (center.get(mm) - point.get(mm)) *
          (center.get(mm) - point.get(mm));
      }
      return dist2;
    }

    public ArrayList<ArrayList<Double>>
      evaluate(ArrayList<ArrayList<Double>> points,
               int K, int max_iterations) throws SemanticException {

      if (K <= 0) {
        throw new UDFArgumentTypeException(K,
                                           "K should be positive.");
      }

      int N = points.size();
      // If we have fewer points than clusters, then just return the input.
      if (N < K) {
        return points;
      }

      // First initialize using kmeans++.
      int M = points.get(0).size();
      ArrayList<ArrayList<Double>> centers = new ArrayList<ArrayList<Double>>();
      double dist2s[] = new double[N];

      for (int ii = 0; ii < K; ++ii) {
        int new_center = -1;

        if (ii > 0) {
          // Compute the distance to all centers.
          for (int jj = 0; jj < N; ++jj) {
            dist2s[jj] = 1e100;
            ArrayList<Double> point = points.get(jj);
            if (point.size() != M) {
              throw new UDFArgumentTypeException(M,
                                                 "Sizes of tuples do not match.");
            }
            for (int kk = 0; kk < ii; ++kk) {
              double dist2 = squared_dist(centers.get(kk), point);
              if (dist2 < dist2s[jj]) {
                dist2s[jj] = dist2;
              }
            }
          }
          // Select a new point.
          new_center = sample(dist2s);
        } else {
          new_center = (int)(Math.random() * N);
        }

        ArrayList<Double> point = points.get(new_center);
        if (point.size() != M) {
          throw new UDFArgumentTypeException(M,
                                             "Sizes of tuples do not match.");
        }
        // Note, we go through the following rigamarole to ensure we have a proper clone.
        ArrayList<Double> new_point = new ArrayList<Double>();
        for (int mm = 0; mm < M; ++mm) {
          new_point.add(point.get(mm).doubleValue());
        }
        centers.add(new_point);
      }

      int[] assignments = new int[N];
      int[] center_counts = new int[K];
      for (int jj = 0; jj < N; ++jj) {
        assignments[jj] = -1;
      }

      for (int ii = 0; ii < max_iterations; ++ii) {
        for (int kk = 0; kk < K; ++kk) {
          center_counts[kk] = 0;
        }
        boolean changed = false;

        // Compute the assignments.
        for (int jj = 0; jj < N; ++jj) {
          int old_assignment = assignments[jj];
          // Find the closest point.
          ArrayList<Double> point = points.get(jj);
          double mindist2 = 1e100;
          for (int kk = 0; kk < K; ++kk) {
            // Compute the distance to the kk'th center.
            double dist2 = squared_dist(centers.get(kk), point);
            if (dist2 < mindist2) {
              assignments[jj] = kk;
              mindist2 = dist2;
            }
          }
          if (assignments[jj] != old_assignment) {
            changed = true;
          }
          center_counts[assignments[jj]]++;
        }

        if (!changed) {
          break;
        }

        // Compute the means.
        for (int kk = 0; kk < K; ++kk) {
          for (int mm = 0; mm < M; ++mm) {
            centers.get(kk).set(mm, 0.0);
          }
        }
        for (int jj = 0; jj < N; ++jj) {
          ArrayList<Double> point = points.get(jj);
          ArrayList<Double> center = centers.get(assignments[jj]);
          for (int mm = 0; mm < M; ++mm) {
            center.set(mm, center.get(mm) +
                       point.get(mm) / center_counts[assignments[jj]]);
          }
        }
      }
      return centers;
    }
  }
