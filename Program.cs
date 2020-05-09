using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace V3Indexer
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            // TODO: RegistrationLeaf should be a URL
            // TODO: NuGetClientFactory should accept a function to create the httpclient
            // TODO: NuGetClientFactory should have an interface.
            ThreadPool.SetMinThreads(workerThreads: 32, completionPortThreads: 4);
            ServicePointManager.DefaultConnectionLimit = 32;
            ServicePointManager.MaxServicePointIdleTime = 10000;

            var hostBuilder = Host.CreateDefaultBuilder(args);

            try
            {
                await hostBuilder
                    .ConfigureServices(ConfigureService)
                    .RunConsoleAsync();
            }
            catch (OperationCanceledException)
            {
            }
        }

        private static void ConfigureService(IServiceCollection services)
        {
            services
                .AddHttpClient("NuGet")
                .ConfigurePrimaryHttpMessageHandler(handler =>
                {
                    return new HttpClientHandler
                    {
                        AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip
                    };
                });

            services.AddSingleton(provider =>
            {
                var factory = provider.GetRequiredService<IHttpClientFactory>();
                var httpClient = factory.CreateClient("NuGet");

                var serviceIndex = "https://api.nuget.org/v3/index.json";

                return new NuGetClientFactory(httpClient, serviceIndex);
            });

            services.AddSingleton<CatalogLeafItemProducer>();
            services.AddSingleton<PackageIdWorker>();

            services.AddHostedService<CatalogProcessor>();
        }
    }
}
