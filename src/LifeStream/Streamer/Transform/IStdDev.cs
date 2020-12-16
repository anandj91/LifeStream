using System;
using Streamer.Ingest;

namespace Microsoft.StreamProcessing
{
    public class StdDevState : IAggState<Signal, float>
    {
        private float[] list;
        private int count;
        private float sum;

        public StdDevState(long period, long window)
        {
            list = new float[window / period];
            count = 0;
            sum = 0;
        }

        public IAggState<Signal, float> Reset()
        {
            count = 0;
            sum = 0;
            return this;
        }

        public IAggState<Signal, float> Acc(IAggState<Signal, float> s, long t, Signal i)
        {
            sum += i.val;
            list[count] = i.val;
            count++;
            return this;
        }

        public float Res()
        {
            var avg = sum / count;
            float var = 0;
            for (int i = 0; i < count; i++)
            {
                var diff = list[i] - avg;
                var += (diff * diff);
            }

            return (float) Math.Sqrt(var / count);
        }
    }

    public class IStdDev : IAgg<Signal, float>
    {
        public IStdDev(long period, long window) : base(new StdDevState(period, window))
        {
        }
    }
}