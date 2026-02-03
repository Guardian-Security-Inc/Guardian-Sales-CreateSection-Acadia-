using System;
using System.Net.Http;
using System.Text;
using System.ServiceModel;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;

public class CreateSectionsAndAccess : IPlugin
{
    public void Execute(IServiceProvider serviceProvider)
    {
        ITracingService tracingService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
        IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
        IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
        IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);

        Entity contract = (Entity)context.InputParameters["Target"];
        Guid contractId = contract.Id;
        Entity retrievedContract = service.Retrieve(context.PrimaryEntityName, contractId, new ColumnSet(true));

        try
        {


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
            string jsonResponse = CallAzureFunction(azureFunctionUrl, jsonPayload, tracingService).Result;

            var completeResponse = JArray.Parse(jsonResponse);
            var sections = completeResponse[0];

            foreach (var section in sections)
            {
                CreateSection(service, section, null, null, context, retrievedContract);
            }
        }
        catch (FaultException<OrganizationServiceFault> ex)
        {
            tracingService.Trace("OrganizationServiceFault: {0}", ex.ToString());
            throw;
        }
        catch (Exception ex)
        {
            tracingService.Trace("Error: {0}", ex.ToString());
            throw;
        }
    }

    private async Task<string> CallAzureFunction(string url, string jsonPayload, ITracingService tracingService)
    {
        using (HttpClient client = new HttpClient())
        {
            try
            {
                HttpContent content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                HttpResponseMessage response = await client.PostAsync(url, content);

                response.EnsureSuccessStatusCode();
                string responseData = await response.Content.ReadAsStringAsync();
                tracingService.Trace("Azure Function Response: " + responseData);
                return responseData;
            }
            catch (Exception ex)
            {
                tracingService.Trace("Error calling Azure Function: {0}", ex.ToString());
                throw;
            }
        }
    }

    
    private void SplitAndSetContent(Entity entity, string fieldName, string content)
    {
       
        const int maxFieldLength = 1048576;

        if (content.Length > maxFieldLength)
        {
            entity[fieldName] = content.Substring(0, maxFieldLength);
            if (content.Length > maxFieldLength * 2)
            {
                entity[fieldName + "1"] = content.Substring(maxFieldLength, maxFieldLength);
                if (content.Length > maxFieldLength * 3)
                {
                    entity[fieldName + "2"] = content.Substring(maxFieldLength * 2, maxFieldLength);
                    if (content.Length > maxFieldLength * 4)
                    {
                        entity[fieldName + "3"] = content.Substring(maxFieldLength * 3, maxFieldLength);
                        if (content.Length > maxFieldLength * 5)
                        {
                            entity[fieldName + "4"] = content.Substring(maxFieldLength * 4, content.Length - maxFieldLength * 4);
                        }
                        else
                        {
                            entity[fieldName + "4"] = content.Substring(maxFieldLength * 4);
                        }
                    }
                    else
                    {
                        entity[fieldName + "3"] = content.Substring(maxFieldLength * 3);
                    }
                }
                else
                {
                    entity[fieldName + "2"] = content.Substring(maxFieldLength * 2);
                }
            }
            else
            {
                entity[fieldName + "1"] = content.Substring(maxFieldLength);
            }
        }
        else
        {
            entity[fieldName] = content;
        }
    }

    
    private void CreateSection(
        IOrganizationService service,
        JToken section,
        Guid? parentSectionId,
        Guid? parentAccessId,
        IPluginExecutionContext context,
        Entity retrievedContract)
    {
        string content = section["Content"]?.ToString();
        string heading = section["Heading"]?.ToString();
        string type = section["Type"]?.ToString();

        // Create the section record
        Entity sectionRecord = new Entity("cm_section");
        SplitAndSetContent(sectionRecord, "cm_descriptionhtml", content); // Split content across multiple fields
        sectionRecord["cm_name"] = heading;

        switch (type)
        {
            case "h1":
                sectionRecord["cm_type"] = new OptionSetValue(121540000);
                break;
            case "h2":
                sectionRecord["cm_type"] = new OptionSetValue(121540001);
                break;
            case "h3":
                sectionRecord["cm_type"] = new OptionSetValue(121540002);
                break;
            case "h4":
                sectionRecord["cm_type"] = new OptionSetValue(121540003);
                break;
            case "h5":
                sectionRecord["cm_type"] = new OptionSetValue(121540004);
                break;
            case "h6":
                sectionRecord["cm_type"] = new OptionSetValue(121540005);
                break;
            default:
                break;
        }

        sectionRecord["cm_contract"] = new EntityReference("cm_contract", context.PrimaryEntityId);

        if (parentSectionId.HasValue)
        {
            sectionRecord["cm_parentsection"] = new EntityReference("cm_section", parentSectionId.Value);
        }

        Guid sectionId = service.Create(sectionRecord);

        
        Entity access = new Entity("cm_access");
        access["cm_section"] = new EntityReference("cm_section", sectionId);
        access["cm_contract"] = new EntityReference("cm_contract", context.PrimaryEntityId);
        access["cm_readonly"] = retrievedContract["cm_document"];
        access["cm_name"] = retrievedContract["cm_cm_templateidentifier"];

       
        access["cm_discriptionhtml"] = sectionRecord["cm_descriptionhtml"];
        access["cm_descriptionhtml1"] = sectionRecord.Contains("cm_descriptionhtml1") ? sectionRecord["cm_descriptionhtml1"] : null;
        access["cm_descriptionhtml2"] = sectionRecord.Contains("cm_descriptionhtml2") ? sectionRecord["cm_descriptionhtml2"] : null;
        access["cm_descriptionhtml3"] = sectionRecord.Contains("cm_descriptionhtml3") ? sectionRecord["cm_descriptionhtml3"] : null;
        access["cm_descriptionhtml4"] = sectionRecord.Contains("cm_descriptionhtml4") ? sectionRecord["cm_descriptionhtml4"] : null;

        if (parentAccessId.HasValue)
        {
            access["cm_parentaccess"] = new EntityReference("cm_access", parentAccessId.Value);
        }

        Guid accessId = service.Create(access);

        // Handle subsections
        if (section["Subsections"] is JArray subsections)
        {
            foreach (var subsection in subsections)
            {
                CreateSection(service, subsection, sectionId, accessId, context, retrievedContract);
            }
        }
    }
}
