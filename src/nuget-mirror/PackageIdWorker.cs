using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BaGet.Protocol;
using BaGet.Protocol.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Mirror
{
    public class PackageIdWorker
    {
        private readonly NuGetClientFactory _factory;
        private readonly JsonSerializer _json;
        private readonly IOptionsSnapshot<MirrorOptions> _options;
        private readonly ILogger<PackageIdWorker> _logger;

        public PackageIdWorker(
            NuGetClientFactory factory,
            IOptionsSnapshot<MirrorOptions> options,
            ILogger<PackageIdWorker> logger)
        {
            _factory = factory;
            _json = new JsonSerializer();
            _options = options;
            _logger = logger;
        }

        public async Task WorkAsync(
            ChannelReader<string> channel,
            CancellationToken cancellationToken)
        {
            var client = _factory.CreatePackageMetadataClient();

            // TODO: This should wait until registration has caught up to the catalog cursor.
            _logger.LogInformation("Indexing package data to path {IndexPath}...", _options.Value.IndexPath);

            var tasks = Enumerable
                .Repeat(0, _options.Value.ConsumerWorkers)
                .Select(async _ =>
                {
                    await Task.Yield();

                    while (await channel.WaitToReadAsync(cancellationToken))
                    {
                        while (channel.TryRead(out var packageId))
                        {
                            _logger.LogDebug("Processing package {PackageId}", packageId);

                            var path = Path.Combine(_options.Value.IndexPath, packageId.ToLowerInvariant() + ".json");

                            var index = await GetInlinedRegistrationIndexOrNullAsync(client, packageId, cancellationToken);
                            if (index == null)
                            {
                                if (File.Exists(path))
                                {
                                    File.Delete(path);
                                }
                            }

                            using var filestream = new FileStream(path, FileMode.Create);
                            using var compressedStream = new GZipStream(filestream, CompressionMode.Compress);
                            using var writer = new StreamWriter(compressedStream);

                            _json.Serialize(writer, index);

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
