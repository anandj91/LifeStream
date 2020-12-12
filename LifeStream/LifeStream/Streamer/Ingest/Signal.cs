using System;

namespace Streamer.Ingest
{
    public struct Signal : IComparable<Signal>
    {
        public long ts;
        public float val;

        public Signal(long ts, float val)
        {
            this.ts = ts;
            this.val = val;
        }

        public int CompareTo(Signal other)
        {
            return (int) (this.ts - other.ts);
        }

        public override string ToString()
        {
            return $"ts={this.ts}, val={this.val}";
        }
    }
}