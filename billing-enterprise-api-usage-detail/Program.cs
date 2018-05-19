using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace billing_enterprise_api_usage_detail
{

    // https://docs.microsoft.com/en-us/rest/api/billing/enterprise/billing-enterprise-api-usage-detail
    class Program
    {
        static async Task Main(string[] args)
        {
            IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();

            var downloadUsageDetailsRequest = new DownloadUsageDetailsRequest
            {
                ForceRun = true,
                AccessToken = config["accessToken"],
                BillingPeriod = "201804",
                EnrollmentNumber = config["enrollmentNumber"]
            };

            var httpClientFactory = new HttpClientFactory();

            var usageDetailsDataProvider = new UsageDetailsDataProvider(httpClientFactory);

            var usageDetails = await usageDetailsDataProvider.GetUsageDetailsAsync(downloadUsageDetailsRequest)
                .ConfigureAwait(false);

            Console.ReadLine();
        }
    }


    public sealed class UsageDetailsChunkComparer
    {
        public static DateTime? GetMinDiffDate(IReadOnlyList<UsageDetailsChunk> chunkMs,
            IReadOnlyList<UsageDetailsChunk> chunkDb)
        {
            var dicMs = chunkMs.GroupBy(q => q.Date).Select(w => new AggregateUsageDetailsChunk
            {
                Date = w.Key,
                CostSum = w.Sum(e => e.Cost),
                Count = w.Count(),
                QuantitySum = w.Sum(e => e.Quantity)
            })
                .OrderBy(q => q.Date)
                .ToDictionary(w => w.Date);

            var dicDb = chunkDb.GroupBy(q => q.Date).Select(w => new AggregateUsageDetailsChunk
            {
                Date = w.Key,
                Count = w.Count(),
                CostSum = w.Sum(e => e.Cost),
                QuantitySum = w.Sum(e => e.Quantity)
            })
                .OrderBy(q => q.Date)
                .ToDictionary(w => w.Date);

            foreach (var usageDetailsChunkMs in dicMs)
            {
                if (!dicDb.TryGetValue(usageDetailsChunkMs.Key, out var usageDetailsChunkDb))
                {
                    return usageDetailsChunkMs.Key;
                }

                if (AreAggregateUsageDetailsChunkEqual(usageDetailsChunkMs.Value, usageDetailsChunkDb))
                {
                    continue;
                }

                return usageDetailsChunkMs.Key;
            }

            return null;
        }

        private static bool AreAggregateUsageDetailsChunkEqual(AggregateUsageDetailsChunk item1, AggregateUsageDetailsChunk item2)
        {
            return item1.Count == item2.Count
                   && item1.CostSum == item2.CostSum
                   && item1.QuantitySum == item2.QuantitySum;
        }
    }

    public struct AggregateUsageDetailsChunk
    {
        public decimal CostSum { get; set; }
        public decimal QuantitySum { get; set; }

        public int Count { get; set; }

        public DateTime Date { get; set; }
    }

    public struct UsageDetailsChunk
    {
        public decimal Cost { get; set; }
        public decimal Quantity { get; set; }

        public DateTime Date { get; set; }
    }

    public abstract class UsageDetailResponseBase
    {
        public UsageDetailItem[] data { get; set; }
    }

    public class UsageDetailResponse : UsageDetailResponseBase
    {
        public string nextLink { get; set; }
        public string ETagHeaderValue { get; set; }

        public bool MakeNextCall => !string.IsNullOrWhiteSpace(nextLink);
    }

    public class NotModifiedUsageDetailResponse : UsageDetailResponse
    {
        public NotModifiedUsageDetailResponse()
        {
            data = new UsageDetailItem[] { };
        }
    }

    public struct UsageDetailItem
    {
        public string accountOwnerEmail { get; set; }
        public string accountName { get; set; }
        public Guid subscriptionGuid { get; set; }
        public string subscriptionName { get; set; }
        public DateTime date { get; set; }
        public string product { get; set; }
        public string meterId { get; set; }
        public string meterCategory { get; set; }
        public string meterSubCategory { get; set; }
        public string meterRegion { get; set; }
        public string meterName { get; set; }
        public decimal consumedQuantity { get; set; }
        public decimal cost { get; set; }
        public string resourceLocation { get; set; }
        public string consumedService { get; set; }
        public string instanceId { get; set; }
        public string tags { get; set; }
        public string departmentName { get; set; }
        public string costCenter { get; set; }
        public string unitOfMeasure { get; set; }
        public string resourceGroup { get; set; }
    }


    public interface IHttpClientFactory
    {
        HttpClient GetClient();
    }

    public sealed class HttpClientFactory : IHttpClientFactory
    {
        private static readonly HttpClientHandler _compressionHandler = new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
        };

        private readonly HttpClient _client = new HttpClient(_compressionHandler)
        {
            BaseAddress = new Uri("https://consumption.azure.com/v2/enrollments/"),
            Timeout = TimeSpan.FromMinutes(15)
        };

        public HttpClient GetClient()
        {
            return _client;
        }
    }

    public interface IUsageDetailsDataProvider
    {
        Task<IList<UsageDetailItem>> GetUsageDetailsAsync(DownloadUsageDetailsRequest request);
    }

    public sealed class UsageDetailsDataProvider : IUsageDetailsDataProvider
    {
        private readonly IHttpClientFactory _httpClientFactory;

        public UsageDetailsDataProvider(IHttpClientFactory httpClientFactory)
        {
            _httpClientFactory = httpClientFactory;
        }

        public async Task<IList<UsageDetailItem>> GetUsageDetailsAsync(DownloadUsageDetailsRequest request)
        {
            var httpClient = _httpClientFactory.GetClient();

            SetCredentials(httpClient, request);

            SetEtagHeader(httpClient, request);

            return await DownloadUsageDataAsync(httpClient, request).ConfigureAwait(false);
        }

        private static async Task<IList<UsageDetailItem>> DownloadUsageDataAsync(HttpClient httpClient, DownloadUsageDetailsRequest request)
        {
            var usageDetails = new List<UsageDetailItem>();

            var data = await DownloadUsageDetailsAsync(httpClient,
                $"{request.EnrollmentNumber}/billingPeriods/{request.BillingPeriod}/usagedetails", request.ForceRun).ConfigureAwait(false);
            usageDetails.AddRange(data.data);
            if (data is NotModifiedUsageDetailResponse)
            {

            }

            if (!string.IsNullOrWhiteSpace(data.ETagHeaderValue))
            {
                // save data.ETagHeaderValue;
            }

            while (data.MakeNextCall)
            {
                data = await DownloadUsageDetailsAsync(httpClient, data.nextLink, request.ForceRun).ConfigureAwait(false);
                usageDetails.AddRange(data.data);
            }

            return usageDetails;
        }

        private static async Task<UsageDetailResponse> DownloadUsageDetailsAsync(HttpClient httpClient, string url, bool forceDownload)
        {
            var response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseContentRead)
                .ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
            if (response.StatusCode == HttpStatusCode.NotModified && !forceDownload)
            {
                return new NotModifiedUsageDetailResponse();
            }

            var result = await Utf8Json.JsonSerializer
                .DeserializeAsync<UsageDetailResponse>(await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                .ConfigureAwait(false);

            //string content = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            // var result = JsonConvert.DeserializeObject<UsageDetailResponse>(content);

            result.ETagHeaderValue = response.Headers.ETag.Tag;

            return result;
        }

        private static void SetEtagHeader(HttpClient httpClient, DownloadUsageDetailsRequest request)
        {
            if (!request.ForceRun && !string.IsNullOrWhiteSpace(request.ETagHeaderValue))
            {
                httpClient.DefaultRequestHeaders.IfNoneMatch.Add(
                    new EntityTagHeaderValue(request.ETagHeaderValue));
            }
        }

        private static void SetCredentials(HttpClient httpClient, DownloadUsageDetailsRequest request)
        {
            httpClient.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", request.AccessToken);
        }
    }


    public struct DownloadUsageDetailsRequest
    {
        public bool ForceRun { get; set; }
        public string EnrollmentNumber { get; set; }
        public string AccessToken { get; set; }

        public string BillingPeriod { get; set; }
        public string ETagHeaderValue { get; set; }
    }
}
