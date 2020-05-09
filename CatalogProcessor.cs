using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace V3Indexer
{
    public class CatalogProcessor : BackgroundService
    {
        private readonly CatalogLeafItemProducer _processor;
        private readonly PackageIdWorker _worker;
        private readonly ILogger<CatalogProcessor> _logger;

        public CatalogProcessor(CatalogLeafItemProducer processor, PackageIdWorker worker, ILogger<CatalogProcessor> logger)
        {
            _processor = processor;
            _worker = worker;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var cursor = DateTimeOffset.MinValue;

            while (!cancellationToken.IsCancellationRequested)
            {
                var stopwatch = Stopwatch.StartNew();
                var leafChannel = Channel.CreateBounded<CatalogLeafItem>(
                    new BoundedChannelOptions(capacity: 128)
                    {
                        FullMode = BoundedChannelFullMode.Wait,
                        SingleWriter = false,
                        SingleReader = true,
                    });
                var packageIdChannel = Channel.CreateBounded<string>(
                    new BoundedChannelOptions(capacity: 128)
                    {
                        FullMode = BoundedChannelFullMode.Wait,
                        SingleWriter = true,
                        SingleReader = false,
                    });

                var producerOptions = new CatalogLeafItemProducerOptions();
                var workerOptions = new PackageIdWorkerOptions();

                producerOptions.MinCursor = cursor;

                var producerTask = _processor.ProduceAsync(leafChannel.Writer, producerOptions, cancellationToken);
                var mapTask = MapLeavesToIdsAsync(leafChannel.Reader, packageIdChannel.Writer, cancellationToken);
                var workerTask = _worker.WorkAsync(packageIdChannel.Reader, workerOptions, cancellationToken);

                await Task.WhenAll(producerTask, mapTask, workerTask);

                cursor = await producerTask;
                _logger.LogInformation(
                    "Processed catalog up to {Cursor} in {ElapsedMinutes} minutes.",
                    cursor,
                    stopwatch.Elapsed.TotalMinutes);

                _logger.LogInformation("Sleeping...");
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            }
        }

        private async Task MapLeavesToIdsAsync(
            ChannelReader<CatalogLeafItem> leafChannel,
            ChannelWriter<string> packageIdChannel,
            CancellationToken cancellationToken)
        {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            while (await leafChannel.WaitToReadAsync(cancellationToken))
            {
                while (leafChannel.TryRead(out var leaf))
                {
                    // Skip package IDs that have already been seen.
                    if (!seen.Add(leaf.PackageId)) continue;

                    if (!packageIdChannel.TryWrite(leaf.PackageId))
                    {
                        await packageIdChannel.WriteAsync(leaf.PackageId);
                    }
                }
            }

            packageIdChannel.Complete();
        }
    }
}
