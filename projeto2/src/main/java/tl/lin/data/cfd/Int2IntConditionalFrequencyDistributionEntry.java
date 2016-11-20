/*
 * Lintools: tools by @lintool
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package tl.lin.data.cfd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import tl.lin.data.fd.Int2IntFrequencyDistribution;
import tl.lin.data.fd.Int2IntFrequencyDistributionEntry;
import tl.lin.data.fd.Int2LongFrequencyDistributionEntry;
import tl.lin.data.map.HMapII;
import tl.lin.data.map.HMapIVW;
import tl.lin.data.pair.PairOfInts;

/**
 * Implementation of {@link Int2IntConditionalFrequencyDistribution} based on {@link HMapII}.
 */
public class Int2IntConditionalFrequencyDistributionEntry implements
    Int2IntConditionalFrequencyDistribution {

  private final HMapIVW<Int2IntFrequencyDistribution> distributions = new HMapIVW<Int2IntFrequencyDistribution>();
  private final Int2LongFrequencyDistributionEntry marginals = new Int2LongFrequencyDistributionEntry();

  private long sumOfAllCounts = 0;

  @Override
  public void set(int k, int cond, int v) {
    if (!distributions.containsKey(cond)) {
      Int2IntFrequencyDistribution fd = new Int2IntFrequencyDistributionEntry();
      fd.set(k, v);
      distributions.put(cond, fd);
      marginals.increment(k, v);

      sumOfAllCounts += v;
    } else {
      Int2IntFrequencyDistribution fd = distributions.get(cond);
      int rv = fd.get(k);

      fd.set(k, v);
      distributions.put(cond, fd);
      marginals.increment(k, -rv + v);

      sumOfAllCounts = sumOfAllCounts - rv + v;
    }
  }

  @Override
  public void increment(int k, int cond) {
    increment(k, cond, 1);
  }

  @Override
  public void increment(int k, int cond, int v) {
    int cur = get(k, cond);
    if (cur == 0) {
      set(k, cond, v);
    } else {
      set(k, cond, cur + v);
    }
  }

  @Override
  public int get(int k, int cond) {
    if (!distributions.containsKey(cond)) {
      return 0;
    }

    return distributions.get(cond).get(k);
  }

  @Override
  public long getMarginalCount(int k) {
    return marginals.get(k);
  }

  @Override
  public Int2IntFrequencyDistribution getConditionalDistribution(int cond) {
    if (distributions.containsKey(cond)) {
      return distributions.get(cond);
    }

    return new Int2IntFrequencyDistributionEntry();
  }

  @Override
  public long getSumOfAllCounts() {
    return sumOfAllCounts;
  }

  @Override
  public void check() {
    Int2IntFrequencyDistribution m = new Int2IntFrequencyDistributionEntry();

    long totalSum = 0;
    for (Int2IntFrequencyDistribution fd : distributions.values()) {
      long conditionalSum = 0;

      for (PairOfInts pair : fd) {
        conditionalSum += pair.getRightElement();
        m.increment(pair.getLeftElement(), pair.getRightElement());
      }

      if (conditionalSum != fd.getSumOfCounts()) {
        throw new RuntimeException("Internal Error!");
      }
      totalSum += fd.getSumOfCounts();
    }

    if (totalSum != getSumOfAllCounts()) {
      throw new RuntimeException("Internal Error! Got " + totalSum + ", Expected "
          + getSumOfAllCounts());
    }

    for (PairOfInts e : m) {
      if (e.getRightElement() != marginals.get(e.getLeftElement())) {
        throw new RuntimeException("Internal Error!");
      }
    }

    for (PairOfInts e : m) {
      if (e.getRightElement() != m.get(e.getLeftElement())) {
        throw new RuntimeException("Internal Error!");
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    marginals.readFields(in);
    distributions.readFields(in);
    sumOfAllCounts = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    marginals.write(out);
    distributions.write(out);
    out.writeLong(sumOfAllCounts);
  }
}
