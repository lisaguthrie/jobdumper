using System.Text.Json;
using System.Text.Json.Nodes;
using System.Web;

HttpClient client = new HttpClient();

// The list of keywords to search for. These should be URL encoded, phrases enclosed in
// quotation marks.
string[] searchKeywords = { "ddjl", "%23DevDiv", "DevDiv", "\"Developer%20Division\"" };

// A dictionary holding all the job listings we've found so far. This allows us to dedupe
// listings across multiple keywords.
Dictionary<string, string> jobs = new Dictionary<string, string>();

// string searchKeyword = "ddjl";
// if (args.Length > 1)
//     searchKeyword = args[1];

// Iterate through each search keyword.
foreach (string searchKeyword in searchKeywords) {
    string searchKeywordDecoded = HttpUtility.UrlDecode(searchKeyword).Replace("\"", "");
    string cacheFileName = searchKeywordDecoded + ".json";
    try {
        // The search results screen accepts a keyword to search for as a URL parameter. No auth required.
        // Sometimes the query randomly returns a 502 error. Retrying usually succeeds.
        HttpResponseMessage response;
        int requestCount = 0;
        do 
        {
            requestCount++;
            response = await client.GetAsync("https://careers.microsoft.com/us/en/search-results?keywords=" + searchKeyword);
            Console.WriteLine("Search keyword '{0}' returned status code {1} on attempt #{2}", searchKeyword, response.StatusCode, requestCount);
            Thread.Sleep(10000 * requestCount);

        } while (!response.IsSuccessStatusCode && requestCount <= 2);
        string responseBody; string listings;

        // If we got a successful response from the query, then use it.
        if (requestCount <= 2) 
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
            
            // Write the identified JSON to a local file, for debugging/replaying purposes.
            await File.WriteAllTextAsync(cacheFileName, listings);
        }

        // If we didn't get a successful response from the query, then fall back to the previously saved results, if available.
        else
        {
            Console.WriteLine("Encountered {0} failures searching for keyword '{1}'. Attempting to read cached results instead.", requestCount, searchKeyword);
            listings = File.ReadAllText(cacheFileName);
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
                if (title.StartsWith ("Senior", StringComparison.CurrentCultureIgnoreCase) || title.StartsWith ("Sr", StringComparison.CurrentCultureIgnoreCase)) careerStage = "Senior";
                if (title.StartsWith ("Principal", StringComparison.CurrentCultureIgnoreCase)) careerStage = "Principal";

                // Write the desired data in CSV format.
                string csvListing = String.Format("{0},{1},{2},{3},{4},{5},{6},https://careers.microsoft.com/us/en/job/{7}", i, searchKeywordDecoded, jobNode["postedDate"].ToString(), title.Replace(',', '-'), location, discipline, careerStage, jobId);

                Console.WriteLine(csvListing);

                // Add into our dictionary so we don't add this job listing twice.
                jobs.Add(jobId, csvListing);
            }
            else {
                Console.WriteLine("Skipping job ID {0} ({1})", jobId, jobNode["title"]);
            }
        }
    }
    catch (Exception e)
    {
        Console.WriteLine("An error occurred for search keyword '{0}': {1}", searchKeyword, e.Message);
    }
}

string resultsFile = String.Format(@"DevDivJobListings{0}.csv", DateTime.Now.ToString("yyyyMMdd"));

try 
{
    using (StreamWriter outputFile = new StreamWriter(resultsFile)) {
        outputFile.WriteLine("Number,SearchKeyword,PostedDate,Title,Location,Discipline,Level,JobPostingUrl");
    // Iterate through the resulting dictionary and write the results out to a file.
        foreach (string jobListing in jobs.Values)
        {
            outputFile.WriteLine(jobListing);   
        }
    }
}
catch (Exception e) 
{
    Console.WriteLine("An error occurred writing to {0}: {1}", resultsFile, e.Message);
}

