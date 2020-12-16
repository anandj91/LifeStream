using Streamer.Ingest;

namespace Microsoft.StreamProcessing
{
    public class AverageState : IAggState<Signal, float>
    {
        private int count;
        private float sum;

        public AverageState()
        {
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
            count++;
            sum += i.val;
            return this;
        }

        public float Res() => sum / count;
    }

    public class IAverage : IAgg<Signal, float>
    {
        public IAverage() : base(new AverageState())
        {
        }
    }
}