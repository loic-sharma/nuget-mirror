using System;
using System.Collections.Generic;
using System.IO;
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
    public class PackageMetadataWorker
    {
        private readonly NuGetClientFactory _factory;
        private readonly JsonSerializer _json;
        private readonly IOptionsSnapshot<MirrorOptions> _options;
        private readonly ILogger<PackageMetadataWorker> _logger;

        public PackageMetadataWorker(
            NuGetClientFactory factory,
            IOptionsSnapshot<MirrorOptions> options,
            ILogger<PackageMetadataWorker> logger)
        {
            _factory = factory;
            _json = new JsonSerializer();
            _options = options;
            _logger = logger;
        }

        public async Task WorkAsync(
            ChannelReader<string> packageIdReader,
            CancellationToken cancellationToken)
        {
            var client = _factory.CreatePackageMetadataClient();
            var metadataPath = Path.Combine(_options.Value.IndexPath, "metadata");

            _logger.LogInformation(
                "Indexing package metadata to path {MetadataPath} using {ConsumerWorkers} workers...",
                metadataPath,
                _options.Value.ConsumerWorkers);

            Directory.CreateDirectory(metadataPath);

            var tasks = Enumerable
                .Repeat(0, _options.Value.ConsumerWorkers)
                .Select(async _ =>
                {
                    await Task.Yield();

                    while (await packageIdReader.WaitToReadAsync(cancellationToken))
                    {
                        while (packageIdReader.TryRead(out var packageId))
                        {
                            var done = false;
                            while (!done)
                            {
                                try
                                {
                                    _logger.LogDebug("Processing package {PackageId}...", packageId);

                                    var path = Path.Combine(metadataPath, packageId.ToLowerInvariant() + ".json");

                                    var index = await GetInlinedRegistrationIndexOrNullAsync(client, packageId, cancellationToken);

                                    using var filestream = new FileStream(path, FileMode.Create);
                                    using var writer = new StreamWriter(filestream);

                                    _json.Serialize(writer, index);
                                    done = true;

                                    _logger.LogDebug("Processed package {PackageId}", packageId);
                                }
                                catch (Exception e) when (!cancellationToken.IsCancellationRequested)
                                {
                                    _logger.LogError(e, "Retrying package {PackageId} in 5 seconds...", packageId);
                                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                                }
                            }

                        }
                    }
                });

            await Task.WhenAll(tasks);

            _logger.LogInformation("Done processing package metadata.");
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
