using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Protocol;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Logging;

namespace V3Indexer
{
    public class PackageIdWorkerOptions
    {
        public int Workers { get; } = 32;
        public string IndexPath { get; }
    }

    public class PackageIdWorker
    {
        private readonly NuGetClientFactory _factory;
        private readonly ILogger<PackageIdWorker> _logger;

        public PackageIdWorker(NuGetClientFactory factory, ILogger<PackageIdWorker> logger)
        {
            _factory = factory;
            _logger = logger;
        }

        public async Task WorkAsync(
            ChannelReader<string> channel,
            PackageIdWorkerOptions options,
            CancellationToken cancellationToken)
        {
            var client = _factory.CreatePackageMetadataClient();

            _logger.LogInformation("Processing packages...");

            var tasks = Enumerable
                .Repeat(0, options.Workers)
                .Select(async _ =>
                {
                    await Task.Yield();

                    while (await channel.WaitToReadAsync(cancellationToken))
                    {
                        while (channel.TryRead(out var packageId))
                        {
                            _logger.LogDebug("Processing package {PackageId}", packageId);

                            //var index = await GetInlinedRegistrationIndexOrNullAsync(client, packageId, cancellationToken);
                            //if (index == null)
                            //{
                            //    _logger.LogWarning("Package {PackageId} has been deleted.", packageId);
                            //}

                            _logger.LogDebug("Processed package {PackageId}", packageId);
                        }
                    }
                });

            await Task.WhenAll(tasks);

            _logger.LogInformation("Done processing packages.");
        }

        private async Task<RegistrationIndexResponse> GetInlinedRegistrationIndexOrNullAsync(
            IPackageMetadataClient client,
            string packageId,
            CancellationToken cancellationToken)
        {
            // Return the index directly if it is not or if all the pages are inlined.
            var index = await client.GetRegistrationIndexOrNullAsync(packageId, cancellationToken);
            if (index == null || index.Pages.All(p => p.ItemsOrNull != null))
            {
                return index;
            }

            // Create a new registration index response with inlined pages.
            var pages = new List<RegistrationIndexPage>();

            foreach (var pageItem in index.Pages)
            {
                if (pageItem.ItemsOrNull == null)
                {
                    var page = await client.GetRegistrationPageAsync(pageItem.RegistrationPageUrl, cancellationToken);

                    pages.Add(page);
                }
            }

            return new RegistrationIndexResponse
            {
                RegistrationIndexUrl = index.RegistrationIndexUrl,
                Type = index.Type,
                Count = index.Count,
                Pages = pages
            };
        }
    }
}
