using System;
using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using BaGet.Protocol;
using BaGet.Protocol.Catalog;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Mirror
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var command = new RootCommand();
            var builder = new CommandLineBuilder(command).UseDefaults();

            command.Description = "Mirror nuget.org package metadata.";
            command.Add(new Argument<DirectoryInfo>
            {
                Name = "path",
                Description = "The directory to store NuGet package metadata.",
            });

            command.Handler = CommandHandler.Create<DirectoryInfo>(async path =>
            {
                // TODO: NuGetClientFactory should accept a function to create the httpclient
                // TODO: NuGetClientFactory should have an interface.
                ThreadPool.SetMinThreads(workerThreads: 32, completionPortThreads: 4);
                ServicePointManager.DefaultConnectionLimit = 32;
                ServicePointManager.MaxServicePointIdleTime = 10000;

                var hostBuilder = Host.CreateDefaultBuilder(args);

                try
                {
                    await hostBuilder
                        .ConfigureServices(services =>
                        {
                            services.Configure<MirrorOptions>(options =>
                            {
                                options.IndexPath = path.FullName;
                            });
                        })
                        .ConfigureServices(ConfigureService)
                        .RunConsoleAsync();
                }
                catch (OperationCanceledException)
                {
                }
            });

            await builder.Build().InvokeAsync(args);
        }

        private static void ConfigureService(HostBuilderContext ctx, IServiceCollection services)
        {
            services.Configure<MirrorOptions>(ctx.Configuration.GetSection("NuGetMirror"));

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

            services.AddSingleton<ICursor>(provider =>
            {
                var options = provider.GetRequiredService<IOptionsSnapshot<MirrorOptions>>();
                var logger = provider.GetRequiredService<ILogger<FileCursor>>();

                var path = Path.Combine(options.Value.IndexPath, "cursor.json");

                return new FileCursor(path, logger);
            });

            services.AddSingleton<CatalogLeafItemProducer>();
            services.AddSingleton<PackageMetadataWorker>();
            services.AddHttpClient<PackageMetadataCursor>();

            services.AddHostedService<MirrorService>();
        }
    }
}
