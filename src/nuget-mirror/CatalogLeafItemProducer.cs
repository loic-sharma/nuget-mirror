using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Protocol;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Mirror
{
    public class CatalogLeafItemProducer
    {
        private readonly NuGetClientFactory _factory;
        private readonly IOptionsSnapshot<MirrorOptions> _options;
        private readonly ILogger<CatalogLeafItemProducer> _logger;

        public CatalogLeafItemProducer(
            NuGetClientFactory factory,
            IOptionsSnapshot<MirrorOptions> options,
            ILogger<CatalogLeafItemProducer> logger)
        {
            _factory = factory;
            _options = options;
            _logger = logger;
        }

        public async Task<DateTimeOffset> ProduceAsync(
            ChannelWriter<CatalogLeafItem> channel,
            DateTimeOffset minCursor,
            DateTimeOffset maxCursor,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation("Fetching catalog index...");
            var client = _factory.CreateCatalogClient();
            var catalogIndex = await client.GetIndexAsync(cancellationToken);

            var pages = catalogIndex.GetPagesInBounds(minCursor, maxCursor);

            var maxPages = _options.Value.MaxPages;
            if (maxPages.HasValue)
            {
                pages = pages.Take(maxPages.Value).ToList();
            }

            if (!pages.Any() || minCursor == maxCursor)
            {
                _logger.LogInformation("No pending leaf items on the catalog.");
                channel.Complete();
                return minCursor;
            }

            var work = new ConcurrentBag<CatalogPageItem>(pages);
            var workers = Math.Min(_options.Value.ProducerWorkers, pages.Count);

            _logger.LogInformation(
                "Fetching {Pages} catalog pages using {ProducerWorkers} workers...",
                pages.Count,
                workers);

            var tasks = Enumerable
                .Repeat(0, workers)
                .Select(async _ =>
                {
                    await Task.Yield();

                    while (work.TryTake(out var pageItem))
                    {
                        var done = false;
                        while (!done)
                        {
                            try
                            {
                                _logger.LogDebug("Processing catalog page {PageUrl}...", pageItem.CatalogPageUrl);
                                var page = await client.GetPageAsync(pageItem.CatalogPageUrl, cancellationToken);

                                foreach (var leaf in page.Items)
                                {
                                    // Don't process leaves that are not within the cursors.
                                    if (leaf.CommitTimestamp <= minCursor) continue;
                                    if (leaf.CommitTimestamp > maxCursor) continue;

                                    if (!channel.TryWrite(leaf))
                                    {
                                        await channel.WriteAsync(leaf, cancellationToken);
                                    }
                                }

                                _logger.LogDebug("Processed catalog page {PageUrl}.", pageItem.CatalogPageUrl);
                                done = true;
                            }
                            catch (Exception e) when (!cancellationToken.IsCancellationRequested)
                            {
                                _logger.LogError(e, "Retrying catalog page {PageUrl} in 5 seconds...", pageItem.CatalogPageUrl);
                                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                            }
                        }
                    }
                });

            await Task.WhenAll(tasks);

            var cursor = pages.Last().CommitTimestamp;
            _logger.LogInformation("Fetched catalog pages up to cursor {Cursor}", cursor);
            channel.Complete();

            return cursor;
        }
    }
}
