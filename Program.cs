using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Protocol;
using BaGet.Protocol.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NuGet.Packaging.Core;

namespace V3Indexer
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            ThreadPool.SetMinThreads(workerThreads: 32, completionPortThreads: 4);
            ServicePointManager.DefaultConnectionLimit = 32;
            ServicePointManager.MaxServicePointIdleTime = 10000;

            var hostBuilder = Host.CreateDefaultBuilder(args);

            await hostBuilder
                .ConfigureServices(ConfigureService)
                .RunConsoleAsync();
        }

        private static void ConfigureService(IServiceCollection services)
        {
            services.AddHttpClient();
            services.AddSingleton(provider =>
            {
                //var factory = provider.GetRequiredService<IHttpClientFactory>();
                //var httpClient = factory.CreateClient("NuGet");

                var httpClient = new HttpClient();
                var serviceIndex = "https://api.nuget.org/v3/index.json";

                return new NuGetClientFactory(httpClient, serviceIndex);
            });

            services.AddHostedService<ProcessCatalogService>();
        }
    }

    public class ProcessCatalogService : BackgroundService
    {
        private readonly NuGetClientFactory _factory;
        private readonly ILogger<ProcessCatalogService> _logger;

        public ProcessCatalogService(
            NuGetClientFactory factory,
            ILogger<ProcessCatalogService> logger)
        {
            _factory = factory;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var cursor = DateTimeOffset.MinValue;

            while (!cancellationToken.IsCancellationRequested)
            {
                var stopwatch = Stopwatch.StartNew();
                var queue = Channel.CreateBounded<string>(
                    new BoundedChannelOptions(capacity: 128)
                    {
                        FullMode = BoundedChannelFullMode.Wait,
                        SingleWriter = false,
                        SingleReader = false,
                    });

                var producer = ProduceWorkAsync(cursor, queue.Writer, cancellationToken);
                var consumer = ConsumeWorkAsync(queue.Reader, cancellationToken);

                await Task.WhenAll(producer, consumer);

                cursor = producer.Result;
                _logger.LogInformation(
                    "Processed catalog up to {Cursor} in {DurationMinutes} minutes.",
                    cursor,
                    stopwatch.Elapsed.TotalMinutes);

                _logger.LogInformation("Sleeping...");
                await Task.Delay(TimeSpan.FromSeconds(30));
                break;
            }
        }

        private async Task<DateTimeOffset> ProduceWorkAsync(
            DateTimeOffset minCursor,
            ChannelWriter<string> queue,
            CancellationToken cancellationToken)
        {
            await Task.Yield();

            var client = _factory.CreateCatalogClient();
            var catalogIndex = await client.GetIndexAsync(cancellationToken);

            var maxCursor = catalogIndex.CommitTimestamp;
            var pages = catalogIndex.GetPagesInBounds(minCursor, maxCursor);

            var work = new ConcurrentBag<CatalogPageItem>(pages);
            var enqueued = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            var producerTasks = Enumerable
                .Repeat(0, Math.Min(16, pages.Count))
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
                                _logger.LogInformation("Processing catalog page {PageUrl}...", pageItem.CatalogPageUrl);

                                var page = await client.GetPageAsync(pageItem.CatalogPageUrl, cancellationToken);
                                var leaves = page.GetLeavesInBounds(minCursor, maxCursor, excludeRedundantLeaves: true);

                                foreach (var leaf in leaves)
                                {
                                    if (enqueued.TryAdd(leaf.PackageId, value: null))
                                    {
                                        await queue.WriteAsync(leaf.PackageId, cancellationToken);
                                    }
                                }

                                done = true;
                            }
                            catch (Exception e) when (!cancellationToken.IsCancellationRequested)
                            {
                                _logger.LogError(e, "Retrying catalog page {PageUrl} in 5 seconds...", pageItem.CatalogPageUrl);
                                await Task.Delay(TimeSpan.FromSeconds(5));
                            }
                        }
                    }
                });

            await Task.WhenAll(producerTasks);

            queue.Complete();

            return maxCursor;
        }

        private async Task ConsumeWorkAsync(ChannelReader<string> queue, CancellationToken cancellationToken)
        {
            var consumerTasks = Enumerable
                .Repeat(0, 16)
                .Select(async _ =>
                {
                    await Task.Yield();

                    while (await queue.WaitToReadAsync(cancellationToken))
                    {
                        while (queue.TryRead(out var packageId))
                        {
                            _logger.LogInformation("Processing {PackageId}...", packageId);
                        }
                    }
                });

            await Task.WhenAll(consumerTasks);
        }
    }

    public static class NuGetExtensions
    {
        public static PackageIdentity ParsePackageIdentity(this CatalogLeafItem leaf)
        {
            return new PackageIdentity(leaf.PackageId, leaf.ParsePackageVersion());
        }
    }
}
