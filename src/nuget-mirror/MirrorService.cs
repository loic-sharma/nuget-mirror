using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Protocol.Catalog;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Mirror
{
    public class MirrorService : BackgroundService
    {
        private readonly CatalogLeafItemProducer _producer;
        private readonly PackageMetadataWorker _worker;
        private readonly PackageMetadataCursor _parentCursor;
        private readonly ICursor _cursor;
        private readonly IOptionsSnapshot<MirrorOptions> _options;
        private readonly ILogger<MirrorService> _logger;

        public MirrorService(
            CatalogLeafItemProducer processor,
            PackageMetadataWorker worker,
            PackageMetadataCursor parentCursor,
            ICursor cursor,
            IOptionsSnapshot<MirrorOptions> options,
            ILogger<MirrorService> logger)
        {
            _producer = processor;
            _worker = worker;
            _parentCursor = parentCursor;
            _cursor = cursor;
            _options = options;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var cursor = await _cursor.GetAsync(cancellationToken) ?? _options.Value.DefaultMinCursor;
            var sleepDuration = TimeSpan.FromSeconds(_options.Value.SleepDurationSeconds);

            while (!cancellationToken.IsCancellationRequested)
            {
                cursor = await ExecuteAsync(cursor, cancellationToken);

                _logger.LogInformation("Sleeping for {SleepDurationSeconds} seconds...", sleepDuration.TotalSeconds);
                await Task.Delay(sleepDuration, cancellationToken);
            }
        }

        private async Task<DateTimeOffset> ExecuteAsync(
            DateTimeOffset minCursor,
            CancellationToken cancellationToken)
        {
            var stopwatch = Stopwatch.StartNew();

            var maxCursor = await _parentCursor.GetAsync(cancellationToken);
            if (minCursor == maxCursor)
            {
                _logger.LogInformation("No pending work");
                return minCursor;
            }

            var leafChannel = Channel.CreateBounded<CatalogLeafItem>(
                new BoundedChannelOptions(capacity: 1024)
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

            _logger.LogInformation(
                "Processing package metadata from {MinCursor} to {MaxCursor}...",
                minCursor,
                maxCursor);

            var producerTask = _producer.ProduceAsync(leafChannel.Writer, minCursor, maxCursor, cancellationToken);
            var mapTask = MapLeavesToIdsAsync(leafChannel.Reader, packageIdChannel.Writer, cancellationToken);
            var workerTask = _worker.WorkAsync(packageIdChannel.Reader, cancellationToken);

            await Task.WhenAll(producerTask, mapTask, workerTask);

            // Save the cursor.
            var cursor = await producerTask;
            await _cursor.SetAsync(cursor, cancellationToken);

            _logger.LogInformation(
                "Processed package metadata from {MinCursor} to {Cursor} in {ElapsedMinutes} minutes.",
                minCursor,
                cursor,
                stopwatch.Elapsed.TotalMinutes);

            return cursor;
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
