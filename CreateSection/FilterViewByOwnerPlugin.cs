using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Crm.Sdk.Messages;
using System;

public class FilterViewByOwnerPlugin : IPlugin
{
    public void Execute(IServiceProvider serviceProvider)
    {
        #region Initialize
        IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
        IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
        IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);
        ITracingService tracingService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
        #endregion

        // Check if the output contains a BusinessEntity (view)
        if (context.OutputParameters.Contains("BusinessEntity"))
        {
            var retrievedResult = (Entity)context.OutputParameters["BusinessEntity"];
            Entity entity = retrievedResult;

            // Replace with your actual view GUID (cm_access view)
            if ((Guid)entity["savedqueryid"] == new Guid("9913c77c-3874-ef11-a671-000d3a9c9af4"))
            {
                // Create a query expression for cm_access entity
                QueryExpression query = new QueryExpression("cm_access");

                // Add condition for current user being the owner
                ConditionExpression ownerCondition = new ConditionExpression
                {
                    AttributeName = "ownerid",
                    Operator = ConditionOperator.Equal,
                    Values = { context.UserId }
                };

                // Add the condition to the query's criteria
                query.Criteria.Conditions.Add(ownerCondition);

                // Convert QueryExpression to FetchXML
                QueryExpressionToFetchXmlRequest fetchRequest = new QueryExpressionToFetchXmlRequest
                {
                    Query = query
                };
                QueryExpressionToFetchXmlResponse fetchResponse = (QueryExpressionToFetchXmlResponse)service.Execute(fetchRequest);

                // Set the modified fetchXML in the view's fetchxml field
                string modifiedFetchXML = fetchResponse.FetchXml;
                entity["fetchxml"] = modifiedFetchXML;

                // Optionally, change the view's name
                // entity["cm_access"] = "Filtered View: Current User Owned Records";
            }
        }
    }
}