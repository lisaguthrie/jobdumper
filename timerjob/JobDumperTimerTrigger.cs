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
        private readonly string[] SEARCHKEYWORDS = { "ddjl", "%23DevDiv", "DevDiv", "\"Developer%20Division\"" };

        [FunctionName("JobDumperTimerTrigger")]
        public async Task Run([TimerTrigger("%JOBDUMPER_CRONEXPRESSION%")]TimerInfo myTimer,  
        [Blob("jobdumper/currentjobs.json", FileAccess.Write)] TextWriter resultsBlob, ILogger log)
        {
            _logger = log;

            _logger.LogInformation($"JobDumperTimerTrigger function executed at: {DateTime.Now}");
            _logger.LogInformation("Cron expression is '{0}'", Environment.GetEnvironmentVariable("JOBDUMPER_CRONEXPRESSION"));

            HttpClient client = new HttpClient();

            // Figure out how many times to retry failed HTTP connections.
            int retries;
            if (!int.TryParse(Environment.GetEnvironmentVariable(ENVVAR_RETRIES), out retries))
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
            Dictionary<string, string> jobs = new Dictionary<string, string>();

            // Iterate through each search keyword.
            foreach (string searchKeyword in searchKeywords) {
                string searchKeywordDecoded = HttpUtility.UrlDecode(searchKeyword).Replace("\"", "");
                try {
                    // The search results screen accepts a keyword to search for as a URL parameter. No auth required.
                    // Sometimes the query randomly returns a 502 error. Retrying usually succeeds.
                    HttpResponseMessage response;
                    int requestCount = 0;
                    do 
                    {
                        requestCount++;
                        response = await client.GetAsync("https://careers.microsoft.com/us/en/search-results?keywords=" + searchKeyword);
                        _logger.LogInformation("Search keyword '{0}' returned status code {1} on attempt #{2}", searchKeyword, response.StatusCode, requestCount);
                        Thread.Sleep(10000 * requestCount);

                    } while (!response.IsSuccessStatusCode && requestCount <= retries);
                    string responseBody; 
                    string listings = String.Empty;

                    // If we got a successful response from the query, then use it.
                    if (requestCount <= retries) 
                    {
                        responseBody = await response.Content.ReadAsStringAsync();

                        // The response is an HTML page, but it contains a large JSON blob that includes all the job listings. It is not
                        // paginated (unlike the actual Careers search results screen).
                        // Specifically, the job listings are in a node called "jobs." As of right now, the node immediately following the
                        // "jobs" node is the "aggregations" node. So we parse through the response to find the exact part of the page's
                        // JSON blob that corresponds to the jobs node.
                        int listingsStartPosition = responseBody.IndexOf(@"{""jobs"":[");
                        int listingsEndPosition = responseBody.IndexOf(@",""aggregations"":[", listingsStartPosition);
                        listings = responseBody.Substring(listingsStartPosition, listingsEndPosition-listingsStartPosition) + "}";
                    }

                    else
                    {
                        _logger.LogError("Encountered {0} failures searching for keyword '{1}'", requestCount, searchKeyword);
                    }

                    // Now that we've gotten rid of the surrounding HTML and are down to just raw JSON, we can load the text into a
                    // JsonNode for easier navigation.
                    JsonNode jobsNode = JsonNode.Parse(listings);

                    // Iterate through the job listings.
                    for (int i=0; i < jobsNode["jobs"].AsArray().Count; i++)
                    {
                        JsonNode jobNode = jobsNode["jobs"][i];

                        // Grab the jobId out of the current job that we're on. This is the 7-digit number that uniquely identifies
                        // a job listing.
                        string jobId = jobNode["jobId"].ToString();

                        // If we haven't seen this job listing previously (under another search keyword), proceed with adding it.
                        if (!jobs.ContainsKey(jobId)) {
                            // Add into our dictionary so we don't add this job listing twice.
                            jobs.Add(jobId, jobNode.ToString());
                        }
                        else {
                            _logger.LogInformation("  Skipping job ID {0} ({1}) - already added from a previous search keyword", jobId, jobNode["title"]);
                        }
                    }
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
            foreach (string jobListing in jobs.Values)
            {
                results += jobListing;
                if (counter < jobs.Values.Count) results += ","; // don't add a comma after the last entry
                results += Environment.NewLine;
                counter++;
            }

            results += "]}";

            await resultsBlob.WriteLineAsync(results);
        }
    }
}
