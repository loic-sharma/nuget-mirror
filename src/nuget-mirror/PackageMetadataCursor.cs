using System;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol;
using Microsoft.Extensions.Logging;

namespace Mirror
{
    public class PackageMetadataCursor
    {
        private static readonly string[] RegistrationBaseUrl = new[] { "RegistrationsBaseUrl" };

        private readonly NuGetClientFactory _factory;
        private readonly HttpClient _httpClient;
        private readonly ILogger<PackageMetadataCursor> _logger;

        private string _cursorUrl;

        public PackageMetadataCursor(
            NuGetClientFactory factory,
            HttpClient httpClient,
            ILogger<PackageMetadataCursor> logger)
        {
            _factory = factory;
            _httpClient = httpClient;
            _logger = logger;

            _cursorUrl = null;
        }

        public async Task<DateTimeOffset> GetAsync(CancellationToken cancellationToken)
        {
            if (_cursorUrl == null)
            {
                _logger.LogDebug("Finding the package metadata cursor...");

                var client = _factory.CreateServiceIndexClient();
                var serviceIndex = await client.GetAsync(cancellationToken);
                var registrationUrl = serviceIndex.GetResourceUrl(RegistrationBaseUrl);

                _cursorUrl = registrationUrl.TrimEnd('/') + "/cursor.json";

                _logger.LogDebug("Found package metadata cursor {PackageMetadataCursorUrl}", _cursorUrl);
            }

            _logger.LogDebug("Fetching the package metadata cursor...");

            var response = await _httpClient.GetFromJsonAsync<CursorResponse>(_cursorUrl, cancellationToken);
            var cursor = DateTime.SpecifyKind(DateTime.Parse(response.Value), DateTimeKind.Utc);

            _logger.LogDebug("Fetched package metadata cursor {PackageMetadataCursor}", cursor);

            return cursor;
        }

        private class CursorResponse
        {
            [JsonPropertyName("value")]
            public string Value { get; set; }
        }
    }
}
