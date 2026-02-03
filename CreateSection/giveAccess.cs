using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk;
using System;

namespace CreateSection
{
    public class giveAccess : IPlugin
    {
        public void Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);
            Entity entity = (Entity)context.InputParameters["Target"];

           
            if (context.MessageName == "Update" && entity.LogicalName == "cm_access")
            {
                if (entity.Contains("cm_team") || entity.Contains("cm_user"))
                {
                    EntityReference team = entity.Contains("cm_team") ? (EntityReference)entity["cm_team"] : null;
                    EntityReference user = entity.Contains("cm_user") ? (EntityReference)entity["cm_user"] : null;

                    if (team != null)
                    {
                        
                        ShareRecordWithTeam(service, entity, team);

                       
                        ShareChildRecords(service, entity.Id, team, null);
                    }

                    if (user != null)
                    {
                       
                        ShareRecordWithUser(service, entity, user);

                        
                        ShareChildRecords(service, entity.Id, null, user);
                    }
                }
            }
        }

        private void ShareRecordWithTeam(IOrganizationService service, Entity entity, EntityReference team)
        {
           
            GrantAccessRequest grantAccess = new GrantAccessRequest
            {
                Target = new EntityReference(entity.LogicalName, entity.Id),
                PrincipalAccess = new PrincipalAccess
                {
                    Principal = team,
                    AccessMask = AccessRights.ReadAccess | AccessRights.WriteAccess
                }
            };
            service.Execute(grantAccess);
        }

        private void ShareRecordWithUser(IOrganizationService service, Entity entity, EntityReference user)
        {
            
            GrantAccessRequest grantAccess = new GrantAccessRequest
            {
                Target = new EntityReference(entity.LogicalName, entity.Id),
                PrincipalAccess = new PrincipalAccess
                {
                    Principal = user,
                    AccessMask = AccessRights.ReadAccess | AccessRights.WriteAccess
                }
            };
            service.Execute(grantAccess);
        }

        private void ShareChildRecords(IOrganizationService service, Guid parentAccessId, EntityReference team, EntityReference user)
        {
            
            QueryExpression query = new QueryExpression("cm_access")
            {
                ColumnSet = new ColumnSet("cm_accessid"),
                Criteria = new FilterExpression
                {
                    Conditions =
            {
                new ConditionExpression("cm_parentaccess", ConditionOperator.Equal, parentAccessId)
            }
                }
            };

            EntityCollection childAccessRecords = service.RetrieveMultiple(query);

            foreach (var childRecord in childAccessRecords.Entities)
            {
                if (team != null)
                {
                   
                    ShareRecordWithTeam(service, childRecord, team);
                }

                if (user != null)
                {
                   
                    ShareRecordWithUser(service, childRecord, user);
                }

               
                ShareChildRecords(service, childRecord.Id, team, user);
            }
        }

    }

}
