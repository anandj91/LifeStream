using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.StreamProcessing;
using Streamer.Ingest;

namespace LifeStream
{
    using test_t = StreamEvent<Signal>;

    public class TestObs : IObservable<test_t>
    {
        public long counter;
        public long duration;
        public int freq;
        public string signal;

        public List<test_t> data;

        public TestObs(string signal, long start, long duration, int freq)
        {
            this.signal = signal;
            this.counter = start;
            this.freq = freq;
            this.duration = duration;
            this.data = new List<test_t>();
            Sample();
        }

        private void Sample()
        {
            var limit = duration * freq;
            for (int i = 0; i < limit; i++)
            {
                var sig = new Signal(this.counter, this.counter);
                data.Add(StreamEvent.CreateInterval(sig.ts, sig.ts + (1000 / freq), sig));
                this.counter += (1000 / freq);
            }
        }

        public IDisposable Subscribe(IObserver<test_t> observer)
        {
            return new Subscription(this, observer);
        }

        private sealed class Subscription : IDisposable
        {
            private readonly TestObs observable;
            private readonly IObserver<test_t> observer;

            public Subscription(TestObs observable, IObserver<test_t> observer)
            {
                this.observer = observer;
                this.observable = observable;
                ThreadPool.QueueUserWorkItem(
                    arg =>
                    {
                        this.Sample();
                        this.observer.OnCompleted();
                    });
            }

            private void Sample()
            {
                for (int i = 0; i < observable.data.Count; i++)
                {
                    this.observer.OnNext(observable.data[i]);
                }
            }

            public void Dispose()
            {
            }
        }
    }
}