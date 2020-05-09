using System;

namespace V3Indexer
{
    public class V3IndexerOptions
    {
        public string IndexPath { get; set; }

        public DateTimeOffset DefaultMinCursor { get; set; } = DateTimeOffset.MinValue;

        public int ProducerWorkers { get; set; } = 32;
        public int ConsumerWorkers { get; set; } = 32;
    }
}
