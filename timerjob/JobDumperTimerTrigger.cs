using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Text.Json.Nodes;
using System.Web;
using System.Net.Http;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs;

namespace JobDumper.TimerTrigger
{
    public class JobDumperTimerTrigger
    {
        private ILogger _logger;

        // Environment variable names
        private string ENVVAR_RETRIES = "JOBDUMPER_RETRIES";
        private string ENVVAR_KEYWORDS = "JOBDUMPER_SEARCHKEYWORDS";

        // Default values for environment variables
        private readonly int RETRIES = 2;
        private readonly string[] SEARCHKEYWORDS = { "%23ddjl", "%23DevDiv", "DevDiv", "\"Developer%20Division\"" };

        private BlobServiceClient _blobServiceClient;

        public JobDumperTimerTrigger()
        {
            _blobServiceClient = new BlobServiceClient(new Uri("https://jobdumper.blob.core.windows.net/"), new DefaultAzureCredential());
        }

        [FunctionName("JobDumperTimerTrigger")]
        public async Task Run([TimerTrigger("%JOBDUMPER_CRONEXPRESSION%")] TimerInfo myTimer, ILogger log)
        //public async Task Run([TimerTrigger("0 * * * * *")] TimerInfo myTimer, ILogger log)
        {
            _logger = log;

            _logger.LogInformation($"JobDumperTimerTrigger function executed at: {DateTime.Now}");
            _logger.LogInformation("Cron expression is '{0}'", Environment.GetEnvironmentVariable("JOBDUMPER_CRONEXPRESSION"));

            var containerClient = _blobServiceClient.GetBlobContainerClient("jobdumper");
            var blobClient = containerClient.GetBlobClient("currentjobs.json");

            using var memoryStream = new MemoryStream();
            using var resultsBlob = new StreamWriter(memoryStream);

            HttpClient client = new();

            // Figure out how many times to retry failed HTTP connections.
            if (!int.TryParse(Environment.GetEnvironmentVariable(ENVVAR_RETRIES), out int retries))
            {
                retries = RETRIES;
                _logger.LogInformation($"Could not load retries from '{ENVVAR_RETRIES}' environment variable. Using default: {RETRIES} retries");
            }
            else
            {
                _logger.LogInformation($"Loaded retries from '{ENVVAR_RETRIES}' environment variable: {retries} retries");
            }

            // The list of keywords to search for. These should be URL encoded, phrases enclosed in
            // (properly escaped) quotation marks.
            string[] searchKeywords = SEARCHKEYWORDS;
            string searchKeywordsFromConfig = Environment.GetEnvironmentVariable(ENVVAR_KEYWORDS);
            if (!String.IsNullOrEmpty(searchKeywordsFromConfig))
            {
                string[] parsedKeywordsFromConfig = searchKeywordsFromConfig.Split(',');
                if (parsedKeywordsFromConfig.Length > 0) searchKeywords = parsedKeywordsFromConfig;
                _logger.LogInformation($"Loaded search keywords from '{ENVVAR_KEYWORDS}' environment variable: {searchKeywordsFromConfig}");
            }
            else
            {
                _logger.LogInformation("Could not load list of search keywords from '{0}' environment variable. Using default: {1}", ENVVAR_KEYWORDS, String.Join(',', SEARCHKEYWORDS));
            }

            // A dictionary holding all the job listings we've found so far. This allows us to dedupe
            // listings across multiple keywords.
            Dictionary<string, JsonNode> jobs = new();

            // Iterate through each search keyword.
            foreach (string searchKeyword in searchKeywords)
            {
                string searchKeywordDecoded = HttpUtility.UrlDecode(searchKeyword).Replace("\"", "");
                try
                {
                    int lastPage = 1; int currentPage = 1;
                    do
                    {
                        // The search results screen accepts a keyword to search for as a URL parameter. No auth required.
                        // Sometimes the query randomly returns a 502 error. Retrying usually succeeds.
                        HttpResponseMessage response;
                        int requestCount = 0;
                        do
                        {
                            requestCount++;
                            response = await client.GetAsync(string.Format($"https://gcsservices.careers.microsoft.com/search/api/v1/search?q={searchKeyword}&pg={currentPage}"));
                            _logger.LogInformation($"Search keyword '{searchKeyword}' (page {currentPage}) returned status code {response.StatusCode} on attempt #{requestCount}");
                            Thread.Sleep(2000 * requestCount);

                        } while (!response.IsSuccessStatusCode && requestCount <= retries);
                        string responseBody = "{}";

                        // If we got a successful response from the query, then use it.
                        if (requestCount <= retries)
                        {
                            responseBody = await response.Content.ReadAsStringAsync();

                            // Conveniently, the response comes back as a json blob.
                            JsonNode jobsNode = JsonNode.Parse(responseBody)["operationResult"]["result"];

                            // Grab the total number of jobs that were found. Jobs come back in batches of 20, so we'll need to keep
                            // repeating this query until we reach the end of the list.
                            if (jobsNode["totalJobs"] != null) lastPage = int.Parse(jobsNode["totalJobs"].ToString()) / 20 + 1;

                            // Now parse the current response and grab the jobs array.
                            if (jobsNode != null)
                            {
                                _logger.LogInformation($"Search keyword '{searchKeyword}' (page {currentPage}/{lastPage}) returned {jobsNode["jobs"].AsArray().Count} jobs");

                                // Iterate through the job listings.
                                for (int i = 0; i < jobsNode["jobs"].AsArray().Count; i++)
                                {
                                    JsonNode jobNode = jobsNode["jobs"][i];

                                    // Grab the jobId out of the current job that we're on. This is the 7-digit number that uniquely identifies
                                    // a job listing.
                                    string jobId = jobNode["jobId"].ToString();

                                    // If we haven't seen this job listing previously (under another search keyword), proceed with adding it.
                                    if (!jobs.ContainsKey(jobId))
                                    {
                                        // Add into our dictionary so we don't add this job listing twice.
                                        jobs.Add(jobId, jobNode);
                                    }
                                    else
                                    {
                                        _logger.LogInformation("  Skipping job ID {0} ({1}) - already added from a previous search keyword", jobId, jobNode["title"]);
                                    }
                                }
                            }
                            else
                            {
                                _logger.LogError("Search keyword '{0}' returned an empty jobs array", searchKeyword);
                            }
                        }

                        else
                        {
                            _logger.LogError("Encountered {0} failures searching for keyword '{1}'", requestCount, searchKeyword);
                        }

                        currentPage++;
                    } while (currentPage <= lastPage);
                }
                catch (Exception e)
                {
                    _logger.LogError("An error occurred for search keyword '{0}': {1}", searchKeyword, e.Message);
                }
            }

            // Now the jobs array holds all the deduped jobs that we found. Dump them into a big JSON node for
            // saving off to an appropriate location.
            string results = $"{{ \"lastUpdated\": \"{DateTime.UtcNow}\", \"jobs\": [";

            int counter = 1;
            _logger.LogInformation("{0} total jobs found", jobs.Values.Count);
            foreach (JsonNode jobListing in jobs.Values)
            {
                // Logic to convert the "new style" Careers job listings so that they're compatible with the old style.
                JsonNode converted = Convert(jobListing);

                results += converted.ToString();
                if (counter < jobs.Values.Count) results += ","; // don't add a comma after the last entry
                results += Environment.NewLine;
                counter++;
            }

            results += "]}";

            await resultsBlob.WriteLineAsync(results);
            await resultsBlob.FlushAsync();
            memoryStream.Position = 0;
            await blobClient.UploadAsync(memoryStream, true);
        }


        // Convert the "new style" Careers job listings so that they are compatible with applications that worked
        // with the old style.
        private JsonNode Convert(JsonNode jobListing)
        {
            JsonObject converted = new()
                    {
                        { "jobId", jobListing["jobId"].ToString() },
                        { "title", jobListing["title"].ToString() },
                        { "postedDate", jobListing["postingDate"].ToString() }
                    };

            // Handle job location(s).
            // Old style: Separate city and country fields. Multi-location listings have an array in multi_location_array.
            // New style: Location information is in properties subnode. primaryLocation is in the format City, State, Country.
            //            Multi-location listings have an array in locations.
            // --------------------
            // Separate primaryLocation into city, state, country
            string[] locationParts = jobListing["properties"]["primaryLocation"].ToString().Split(',');
            converted.Add("city", locationParts[0].Trim());
            converted.Add("state", locationParts[1].Trim());
            converted.Add("country", locationParts[2].Trim());
            // Also save the entire unprocessed primaryLocation string as a subnode. (Old-style apps will ignore it.)
            converted.Add("primaryLocation", jobListing["properties"]["primaryLocation"].ToString());
            // Save locations array to multi_location_array
            if (jobListing["properties"]["locations"] != null)
            {
                converted.Add("multi_location_array", JsonArray.Parse(jobListing["properties"]["locations"].ToString()));
            }
            else
            {
                converted.Add("multi_location_array", new JsonArray());
            }

            // Handle disciplines.
            // Old style: Discipline is in the subCategory node.
            // New style: Discipline is in the discipline subnode of the properties node.
            converted.Add("subCategory", jobListing["properties"]["discipline"].ToString());

            // Create a url field, which is a link to the job listing on the Careers site. This way, we can update
            // the logic in this one place, and downstream applications can just use the url.
            converted.Add("url", $"https://jobs.careers.microsoft.com/us/en/job/{jobListing["jobId"].ToString()}");

            // Pull all the other properties out of the properties subnode, and save them as top-level properties.
            foreach (KeyValuePair<string, JsonNode> property in jobListing["properties"].AsObject())
            {
                if (!converted.ContainsKey(property.Key))
                {
                    string value = String.Empty;
                    if (property.Value != null) value = property.Value.ToString();
                    converted.Add(property.Key, value);
                }
            }

            return converted;
        }
    }
}
