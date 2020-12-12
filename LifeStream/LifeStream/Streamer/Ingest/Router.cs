using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Streamer.Ingest.Parser;

namespace Streamer.Ingest
{
    public class Router<T> : IObservable<T>, IObserver<EventParser<T>>
    {
        private readonly BlockingCollection<EventParser<T>> iqueue;
        private readonly BlockingCollection<Task<IEnumerable<T>>> oqueue;

        public Router(int capacity = -1)
        {
            iqueue = new BlockingCollection<EventParser<T>>();
            oqueue = (capacity > 0)
                ? new BlockingCollection<Task<IEnumerable<T>>>(capacity)
                : new BlockingCollection<Task<IEnumerable<T>>>();

            ThreadPool.QueueUserWorkItem(arg =>
            {
                try
                {
                    while (true)
                    {
                        var parser = iqueue.Take();
                        var t = Task.Run(parser.Parse);
                        oqueue.Add(t);
                    }
                }
                catch (InvalidOperationException e)
                {
                    oqueue.CompleteAdding();
                }
            });
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            var disposable = new Unsubscriber(observer, oqueue);
            disposable.Start();
            return disposable;
        }

        internal class Unsubscriber : IDisposable
        {
            private readonly BlockingCollection<Task<IEnumerable<T>>> queue;
            private readonly IObserver<T> observer;

            internal Unsubscriber(IObserver<T> observer, BlockingCollection<Task<IEnumerable<T>>> queue)
            {
                this.observer = observer;
                this.queue = queue;
            }

            internal void Start()
            {
                ThreadPool.QueueUserWorkItem(async arg =>
                {
                    try
                    {
                        while (!queue.IsCompleted)
                        {
                            var top = queue.Take();
                            var ts = await top;
                            foreach (var t in ts)
                            {
                                observer.OnNext(t);
                            }
                        }

                        observer.OnCompleted();
                    }
                    catch (InvalidOperationException e)
                    {
                        observer.OnCompleted();
                    }
                });
            }

            public void Dispose()
            {
            }
        }

        public void OnCompleted()
        {
            iqueue.CompleteAdding();
        }

        public void OnError(Exception error)
        {
            iqueue.CompleteAdding();
        }

        public void OnNext(EventParser<T> value)
        {
            iqueue.Add(value);
        }
    }
}