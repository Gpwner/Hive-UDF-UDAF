package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;

import java.util.ArrayList;

/**
 * Performs LDA inference on documents, given fixed topics.
 */
@Description(name = "udflda",
             value = "_FUNC_(words, topics, initial, alpha, num_iterations) - Perform LDA inference on a document given by 0-indexed words using the topics (which should be properly normalized and smoothed.  Returns the topic proportions.")

  public class UDFLDA extends UDF {
    public ArrayList<Double> evaluate(ArrayList<Integer> words, ArrayList<Double> topics,
                                      ArrayList<Double> initial, double alpha,
                             int num_iterations) {
      int K = initial.size();
      int Nw = words.size();

      double[] document_sum = new double[K];
      double[] assignments = new double[K * Nw];

	  // Initialize document_sum
      for (int kk = 0; kk < K; ++kk) {
        document_sum[kk] = initial.get(kk) * Nw;
        for (int ww = 0; ww < Nw; ++ww) {
          assignments[kk + ww * K] = initial.get(kk);
        }
      }

      for (int ii = 0; ii < num_iterations; ++ii) {
        for (int ww = 0; ww < Nw; ++ww) {
          int word = words.get(ww);
          double w_sum = 0.0;
          for (int kk = 0; kk < K; ++kk) {
            document_sum[kk] -= assignments[kk + ww * K];
          }
          for (int kk = 0; kk < K; ++kk) {
            assignments[kk + ww * K] = (document_sum[kk] + alpha) * topics.get(kk + word * K);
            w_sum += assignments[kk + ww * K];
          }
          for (int kk = 0; kk < K; ++kk) {
            assignments[kk + ww * K] /= w_sum;
            document_sum[kk] -= assignments[kk + ww * K];
          }
        }
      }

      // Normalize document_sum
      double sum = 0.0;
      for (int kk = 0; kk < K; ++kk) {
        sum += document_sum[kk];
      }
      ArrayList<Double> result = new ArrayList<Double>(K);
      for (int kk = 0; kk < K; ++kk) {
        if (sum == 0.0) {
          result.add(1.0 / K);
        } else {
          result.add(document_sum[kk] / sum);
        }
      }
      return result;
    }
  }
