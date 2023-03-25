using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text.Json.Nodes;
using System.Text;

namespace JobDumper.LandingPage
{
    public static class AllJobsApi
    {
        // Location of the blob file containing job listings
        private const string LISTINGSBLOB = "jobdumper/currentjobs.json";

        [FunctionName("AllJobsApi")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, [Blob(LISTINGSBLOB, FileAccess.Read)] string listings,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            StringBuilder outputString = new StringBuilder();

            outputString.AppendLine("Number,PostedDate,Title,Location,Discipline,Level,JobPostingUrl");

            JsonNode jobsNode = JsonNode.Parse(listings);

            if (jobsNode != null) {
                // Iterate through the job listings.
                for (int i=0; i < jobsNode["jobs"].AsArray().Count; i++)
                {
                    try {
                        JsonNode jobNode = jobsNode["jobs"][i];

                        // Parsing location. Most US jobs should be posted with a list of cities associated.
                        // Then we just use the country as the location. If there is just one city associated
                        // with a US listing, then we assume it really is just that city.
                        string location = jobNode["country"].ToString();
                        if (jobNode["multi_location_array"].AsArray().Count == 1 && jobNode["city"].ToString() != "Multiple Locations")
                            location = jobNode["city"].ToString();

                        // Parsing the discipline. We base this on the title of the job listing.
                        string title = jobNode["title"].ToString();
                        string discipline = "Software Engineering";
                        // Look for PM positions. This also includes TPM. These strings will match
                        // "Manager" as well as Management"
                        if (title.Contains("Product Manage") || title.Contains("Program Manage")) discipline = "Program Management";
                        // Reconcile research scientist positions under the general "Data Science" discipline.
                        // Sometimes the title is "Research Science," sometimes it's "Research Scientist."
                        else if (title.Contains("Research Scien")) discipline = "Data Science";
                        else discipline = jobNode["subCategory"].ToString();

                        // Parsing the career stage. Also based on the listing title.
                        string careerStage = "Entry Level";
                        if (title.Contains("Lead", StringComparison.CurrentCultureIgnoreCase)) careerStage = "Senior"; // assume leads are Senior, unless they match the Principal criteria
                        if (title.StartsWith ("Senior", StringComparison.CurrentCultureIgnoreCase) || title.StartsWith ("Sr", StringComparison.CurrentCultureIgnoreCase)) careerStage = "Senior";
                        if (title.StartsWith ("Principal", StringComparison.CurrentCultureIgnoreCase)) careerStage = "Principal";

                        // Write the desired data in CSV format.
                        outputString.AppendLine($"{i},{jobNode["postedDate"].ToString()},{title.Replace(',', '-')},{location},{discipline},{careerStage},https://careers.microsoft.com/us/en/job/{jobNode["jobId"].ToString()}");
                    }
                    catch (Exception ex)
                    {
                        log.LogError($"Error parsing job #{i}: {ex.Message}");
                    }
                }
            }

            return new OkObjectResult(outputString.ToString());
        }
    }
}
