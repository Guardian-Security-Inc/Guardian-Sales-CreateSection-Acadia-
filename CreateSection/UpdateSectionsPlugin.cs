using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;

public class UpdateSectionsPlugin : IPlugin
{
    public void Execute(IServiceProvider serviceProvider)
    {
       
        ITracingService tracingService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
        tracingService.Trace("Begin Plugin Execution");
        IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));

        
        IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
        IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);

     
        if (!context.InputParameters.Contains("RecordGuidId") || !(context.InputParameters["RecordGuidId"] is string recordGuidIdString) || !Guid.TryParse(recordGuidIdString, out Guid recordGuidId))
        {
            throw new InvalidPluginExecutionException("RecordGuidId is required and should be a valid GUID string.");
        }

        try
        {
           
            Guid documentId = recordGuidId;

            //Entity retrievedContract = service.Retrieve(context.PrimaryEntityName, documentId, new ColumnSet(true));// yaha primary entity name nahi ayega
            Entity retrievedContract = service.Retrieve("cm_contract", documentId, new ColumnSet(true));// yaha primary entity name nahi ayega
            QueryExpression query = new QueryExpression("cm_section");
            query.Criteria.AddCondition("cm_contract", ConditionOperator.Equal, documentId);
            query.ColumnSet = new ColumnSet("cm_name");

            EntityCollection sectionsCollection = service.RetrieveMultiple(query);
            tracingService.Trace($"Retrieved {sectionsCollection.Entities.Count} sections from Dataverse.");

            QueryExpression configurationquery = new QueryExpression("cm_acadiaaiconfiguration")
            {
                ColumnSet = new ColumnSet(true),
                TopCount = 1
            };
            EntityCollection resultconfigurationquery = service.RetrieveMultiple(configurationquery);

            if (resultconfigurationquery.Entities.Count <= 0)
            {
                return;
            }

            Entity retrievedConfiguration = resultconfigurationquery.Entities[0];

     

            string azureFunctionUrl = retrievedConfiguration.Contains("cm_extractdocumentheadingbaseurl")
                           ? retrievedConfiguration["cm_extractdocumentheadingbaseurl"].ToString() : null;

            string documentName = retrievedContract.Contains("cm_cm_templateidentifier")
                       ? retrievedContract["cm_cm_templateidentifier"].ToString() : null;
            string clientId = retrievedConfiguration.Contains("cm_sharepointclientid")
                              ? retrievedConfiguration["cm_sharepointclientid"].ToString() : null;
            string clientSecret = retrievedConfiguration.Contains("cm_sharepointclientsecret")
                                  ? retrievedConfiguration["cm_sharepointclientsecret"].ToString() : null;
            string readSiteId = retrievedConfiguration.Contains("cm_sharepointsiteid")
                                ? retrievedConfiguration["cm_sharepointsiteid"].ToString() : null;
            string tenantId = retrievedConfiguration.Contains("cm_chatbottenantid")
                              ? retrievedConfiguration["cm_chatbottenantid"].ToString() : null;
            string readDriveId = retrievedConfiguration.Contains("cm_sharepointdriveid")
                                 ? retrievedConfiguration["cm_sharepointdriveid"].ToString() : null;


            if (documentName == null || clientId == null || clientSecret == null ||
                readSiteId == null || tenantId == null || readDriveId == null || azureFunctionUrl == null)
            {
                tracingService.Trace("One or more required fields are missing in the configuration record.");
                throw new InvalidPluginExecutionException("Missing required fields in the configuration record.");
            }

            var requestBody = new
            {
                document_name = documentName,
                client_id = clientId,
                client_secret = clientSecret,
                read_site_id = readSiteId,
                tenant_id = tenantId,
                read_drive_id = readDriveId
            };

            string jsonPayload = JsonConvert.SerializeObject(requestBody);
           

            using (HttpClient client = new HttpClient())
            {
                HttpContent content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                HttpResponseMessage response = client.PostAsync(azureFunctionUrl, content).GetAwaiter().GetResult();

                response.EnsureSuccessStatusCode();
                string responseData = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                tracingService.Trace("Azure Function Response: " + responseData);

                // Parse the JSON response
                var completeResponse = JArray.Parse(responseData);
                var headingsArray = completeResponse[1] as JArray;

                // Convert JSON headings to a list of strings
                List<string> headings = headingsArray.ToObject<List<string>>();

                // Update the Dataverse records with the headings
                foreach (var dbSection in sectionsCollection.Entities)
                {
                    string dbSectionName = dbSection.GetAttributeValue<string>("cm_name");
                    string dbSectionText = StripNumbering(dbSectionName);

                    // Match the section from the Azure Function by heading name (ignoring numbers)
                    foreach (var heading in headings)
                    {

                        string azureHeadingText = StripNumbering(heading);

                        // Compare the text without numbers to find a match
                        if (dbSectionText.Equals(azureHeadingText, StringComparison.OrdinalIgnoreCase))
                        {
                            // Update the section heading with the new heading (preserve the full heading including the number)
                            dbSection["cm_name"] = heading; // Include full heading (with number)
                            service.Update(dbSection);  // Save to Dataverse
                            tracingService.Trace($"Updated section: {heading}");
                            break;
                        }
                    }
                }

                tracingService.Trace("All sections updated successfully.");
            }
        }
        catch (Exception ex)
        {
            tracingService.Trace("Exception: " + ex.Message);
            throw new InvalidPluginExecutionException(ex.Message);
        }
    }

    // Helper function to strip the numbering (e.g. "3.1.2 Scope" becomes "Scope")
    // Helper function to strip the numbering (e.g. "3.1.2 Scope" becomes "Scope")
    // Helper function to strip the numbering (e.g. "3.1.2 Scope" becomes "Scope")
    private string StripNumbering(string heading)
    {
        return Regex.Replace(heading, @"^[^\p{L}]*", string.Empty);
    }



}
