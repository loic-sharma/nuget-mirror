using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Protocol;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Logging;

namespace V3Indexer
{
    public class CatalogLeafItemProducerOptions
    {
        public DateTimeOffset MinCursor { get; set; } = DateTimeOffset.MinValue;
        public int Workers { get; }  = 32;
    }

    public class CatalogLeafItemProducer
    {
        private readonly NuGetClientFactory _factory;
        private readonly ILogger<CatalogLeafItemProducer> _logger;

        public CatalogLeafItemProducer(NuGetClientFactory factory, ILogger<CatalogLeafItemProducer> logger)
        {
            _factory = factory;
            _logger = logger;
        }

        public async Task<DateTimeOffset> ProduceAsync(
            ChannelWriter<CatalogLeafItem> channel,
            CatalogLeafItemProducerOptions options,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation("Fetching catalog index...");
            var client = _factory.CreateCatalogClient();
            var catalogIndex = await client.GetIndexAsync(cancellationToken);

            var maxCursor = catalogIndex.CommitTimestamp;
            var pages = catalogIndex.GetPagesInBounds(options.MinCursor, maxCursor);

            if (!pages.Any() || options.MinCursor == maxCursor)
            {
                _logger.LogInformation("No pending leaf items on the catalog.");
                channel.Complete();
                return maxCursor;
            }

            var work = new ConcurrentBag<CatalogPageItem>(pages);
            var tasks = Enumerable
                .Repeat(0, Math.Min(options.Workers, pages.Count))
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
                                    if (leaf.CommitTimestamp <= options.MinCursor) continue;
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

            _logger.LogInformation("Fetched catalog pages up to cursor {Cursor}", maxCursor);
            channel.Complete();
            return maxCursor;
        }
    }
}
