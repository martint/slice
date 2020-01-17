/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.slice;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkReadDoubles
{
    @Benchmark
    @OperationsPerInvocation(BenchmarkData.COUNT)
    public Object sliceInputTwoAtATime(BenchmarkData data)
    {
        double[] result = data.result;
        BasicSliceInput input = data.slice.getInput();
        for (int i = 0; i < BenchmarkData.COUNT / 2; i++) {
            result[2 * i] = input.readDouble();
            result[2 * i + 1] = input.readDouble();
        }

        return result;
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkData.COUNT)
    public Object benchmarkSliceInput(BenchmarkData data)
    {
        double[] result = data.result;
        BasicSliceInput input = data.slice.getInput();
        int count = data.count;

        for (int i = 0; i < count; i++) {
            result[i] = input.readDouble();
        }

        return result;
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkData.COUNT)
    public Object benchmarkSimulatedSliceInput(BenchmarkData data)
    {
        Slice slice = data.slice;
        double[] result = data.result;
        int position = 0;
        for (int i = 0; i < BenchmarkData.COUNT; i++) {
            result[i] = slice.getDouble(position);
            position += SIZE_OF_DOUBLE;
        }

        return result;
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkData.COUNT)
    public Object benchmarkSimulatedSliceInputUnchecked(BenchmarkData data)
    {
        Slice slice = data.slice;
        double[] result = data.result;
        int position = 0;
        for (int i = 0; i < BenchmarkData.COUNT; i++) {
            result[i] = slice.getDoubleUnchecked(position);
            position += SIZE_OF_DOUBLE;
        }

        return result;
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkData.COUNT)
    public Object benchmarkChecked(BenchmarkData data)
    {
        Slice slice = data.slice;
        double[] result = data.result;
        int count = data.count;

        for (int i = 0; i < count; i++) {
            result[i] = slice.getDouble(i * SIZE_OF_DOUBLE);
        }

        return result;
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkData.COUNT)
    public Object benchmarkUnchecked(BenchmarkData data)
    {
        Slice slice = data.slice;
        double[] result = data.result;
        int count = data.count;

        for (int i = 0; i < count; i++) {
            result[i] = slice.getDoubleUnchecked(i * SIZE_OF_DOUBLE);
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(BenchmarkData.COUNT)
    public Object benchmarkCheckedSimulated(BenchmarkData data)
    {
        Slice slice = data.slice;
        double[] result = data.result;
        int length = slice.length();
        for (int i = 0; i < BenchmarkData.COUNT; i++) {
            int position = i * SIZE_OF_DOUBLE;
            checkPosition(length, position);
            result[i] = slice.getDoubleUnchecked(position);
        }
        return result;
    }

    private void checkPosition(int length, int position)
    {
        if (position < 0 || position + 8 > length) {
            throw new IndexOutOfBoundsException();
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int COUNT = 1_000_000;
        private int count;
        private int size;

        private double[] result;
        private Slice slice;

        @Setup(Level.Iteration)
        public void setup()
        {
            size = COUNT * SizeOf.SIZE_OF_LONG;
            count = COUNT;

            byte[] data = new byte[size];
            ThreadLocalRandom.current().nextBytes(data);
            slice = Slices.wrappedBuffer(data);

            result = new double[COUNT];
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkReadDoubles.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
