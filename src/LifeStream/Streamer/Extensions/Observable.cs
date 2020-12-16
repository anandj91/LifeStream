using Streamer.Ingest;
using Streamer.Ingest.Parser;

namespace System.Reactive.Linq
{
    public static partial class Observable
    {
        public static IObservable<T> Parse<T>(this IObservable<EventParser<T>> source, int capacity = -1)
        {
            var router = new Router<T>(capacity);
            source.Subscribe(router);
            return router;
        }
    }
}