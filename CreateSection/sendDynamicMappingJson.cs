using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CreateSection
{
    public class sendDynamicMappingJson : IPlugin
    {
        private const int MaxDepth = 10;

        public void Execute(IServiceProvider serviceProvider)
        {
            ITracingService tracingService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));
            tracingService.Trace("Begin Plugin Execution");

            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);

            try
            {
                if (!context.InputParameters.Contains("DynamicTemplateGuid") ||
                    !(context.InputParameters["DynamicTemplateGuid"] is string templateGuidString) ||
                    !Guid.TryParse(templateGuidString, out Guid templateGuid))
                    throw new InvalidPluginExecutionException("Input parameter 'DynamicTemplateGuid' is required and must be a valid GUID string.");

                if (!context.InputParameters.Contains("mainTableLogicalNAme") || context.InputParameters["mainTableLogicalNAme"] == null)
                    throw new InvalidPluginExecutionException("Input parameter 'mainTableLogicalName' is required.");
                string mainTableLogicalName = context.InputParameters["mainTableLogicalNAme"].ToString();

                if (!context.InputParameters.Contains("MainTableGuid") ||
                    !(context.InputParameters["MainTableGuid"] is string mainTableGuidString) ||
                    !Guid.TryParse(mainTableGuidString, out Guid mainTableGuid))
                    throw new InvalidPluginExecutionException("Input parameter 'mainTableGuid' is required and must be a valid GUID string.");

                if (!context.InputParameters.Contains("documentJson") || context.InputParameters["documentJson"] == null)
                    throw new InvalidPluginExecutionException("Input parameter 'documentJson' is required.");
                string documentJsonString = context.InputParameters["documentJson"].ToString();
                JObject documentJson = JObject.Parse(documentJsonString);

                tracingService.Trace($"Inputs OK. Template={templateGuid}, MainTable={mainTableLogicalName}, MainTableId={mainTableGuid}");

                Entity mainRecord = service.Retrieve(mainTableLogicalName, mainTableGuid, new ColumnSet(true));

                JArray mappingDataArray = new JArray();

                var topLevelMappings = RetrieveMappings(service, templateGuid, null, false, tracingService);

                var noSectionMappings = topLevelMappings.Entities
                    .Where(m => m.GetAttributeValue<EntityReference>("cm_tablesection") == null)
                    .ToList();

                JObject allPlaceholders = new JObject();
                var visitedNoSection = new HashSet<Guid>();

                foreach (var mapping in noSectionMappings)
                {
                    ProcessMappingRecursive(service, tracingService, mapping, mainRecord, allPlaceholders, visitedNoSection, depth: 0);
                }

                if (noSectionMappings.Count > 0)
                {
                    mappingDataArray.Add(new JObject
                    {
                        ["isTable"] = false,
                        ["placeholders"] = allPlaceholders
                    });
                }

                var groupedSections = topLevelMappings.Entities
                    .Where(m => m.GetAttributeValue<EntityReference>("cm_tablesection") != null)
                    .GroupBy(m => m.GetAttributeValue<EntityReference>("cm_tablesection")?.Id)
                    .ToList();

                foreach (var group in groupedSections)
                {
                    var tableSectionRef = group.First().GetAttributeValue<EntityReference>("cm_tablesection");
                    string heading = tableSectionRef?.Name ?? "Section:";

                    JObject sectionPlaceholders = new JObject();
                    var visitedSection = new HashSet<Guid>();

                    foreach (var mapping in group)
                    {
                        ProcessMappingRecursive(service, tracingService, mapping, mainRecord, sectionPlaceholders, visitedSection, depth: 0);
                    }

                    mappingDataArray.Add(new JObject
                    {
                        ["heading"] = heading,
                        ["isTable"] = true,
                        ["placeholders"] = sectionPlaceholders
                    });
                }

                var tableMappings = RetrieveMappings(service, templateGuid, null, true, tracingService);

                if (tableMappings.Entities.Count > 0)
                {
                    var groupedByTableSection = tableMappings.Entities
                        .GroupBy(m => m.GetAttributeValue<EntityReference>("cm_tablesection")?.Id)
                        .ToList();

                    foreach (var group in groupedByTableSection)
                    {
                        var tableSectionRef = group.First().GetAttributeValue<EntityReference>("cm_tablesection");
                        string heading = tableSectionRef?.Name ?? "Table:";

                        string childEntityName = group.First().GetAttributeValue<string>("cm_tablelogicalname");
                        if (string.IsNullOrWhiteSpace(childEntityName))
                        {
                            tracingService.Trace($"Skipping table group {heading} because cm_tablelogicalname is empty.");
                            continue;
                        }

                        string parentLookupField = group.FirstOrDefault(g => !string.IsNullOrWhiteSpace(g.GetAttributeValue<string>("cm_parententitylogicalnameonlineitem")))
                            ?.GetAttributeValue<string>("cm_parententitylogicalnameonlineitem");

                        if (string.IsNullOrWhiteSpace(parentLookupField))
                        {
                            tracingService.Trace($"Skipping table group {heading} because cm_parententitylogicalnameonlineitem is missing.");
                            continue;
                        }

                        QueryExpression qeChild = new QueryExpression(childEntityName)
                        {
                            ColumnSet = new ColumnSet(true),
                            Criteria = new FilterExpression(LogicalOperator.And)
                        };
                        qeChild.Criteria.AddCondition(parentLookupField, ConditionOperator.Equal, mainRecord.Id);

                        // Check template mapping for product type filter
                        OptionSetValue mappingProductType = group
                            .Select(m => m.GetAttributeValue<OptionSetValue>("imd_producttype"))
                            .FirstOrDefault(v => v != null);

                        // Check template mapping for product family (for test and inspect only)
                        EntityReference mappingProductFamilyRef = group.FirstOrDefault()?.GetAttributeValue<EntityReference>("imd_productfamily");

                        if (mappingProductType != null)
                        {
                            tracingService.Trace($"Adding imd_producttype filter for table '{heading}' with value {mappingProductType.Value}");
                            LinkEntity productLink = new LinkEntity(childEntityName, "product", "productid", "productid", JoinOperator.Inner);
                            productLink.LinkCriteria = new FilterExpression(LogicalOperator.And);
                            productLink.LinkCriteria.AddCondition("imd_producttype", ConditionOperator.Equal, mappingProductType.Value);
                            qeChild.LinkEntities.Add(productLink);
                        }

                        EntityCollection childRecords;
                        try
                        {
                            childRecords = service.RetrieveMultiple(qeChild);
                            tracingService.Trace($"Retrieved {childRecords.Entities.Count} child records for table '{heading}' after product type filter");
                        }
                        catch (Exception ex)
                        {
                            tracingService.Trace($"Error retrieving child records for {childEntityName} using {parentLookupField}: {ex.Message}");
                            continue;
                        }

                        // For test and inspect (826080004): Additional filtering by product family if specified
                        HashSet<Guid> validProductIds = null;
                        if (mappingProductType != null && mappingProductType.Value == 826080004 && mappingProductFamilyRef != null)
                        {
                            tracingService.Trace($"Test and inspect detected - filtering by product family {mappingProductFamilyRef.Id}");
                            var descendantIds = GetAllDescendantProducts(service, mappingProductFamilyRef.Id, tracingService);
                            validProductIds = new HashSet<Guid>(descendantIds);
                            tracingService.Trace($"Found {validProductIds.Count} descendant products from family {mappingProductFamilyRef.Id}");
                        }

                        JArray tableRows = new JArray();
                        int recordsIncluded = 0;
                        int recordsFilteredOut = 0;

                        foreach (var childRecord in childRecords.Entities)
                        {
                            // For test and inspect with product family: verify product is in family hierarchy
                            if (validProductIds != null && validProductIds.Count > 0)
                            {
                                if (childRecord.Contains("productid") && childRecord["productid"] is EntityReference productRef)
                                {
                                    if (!validProductIds.Contains(productRef.Id))
                                    {
                                        recordsFilteredOut++;
                                        tracingService.Trace($"Filtered out quote detail {childRecord.Id} - product {productRef.Id} is not a descendant of product family {mappingProductFamilyRef.Id}");
                                        continue;
                                    }
                                    recordsIncluded++;
                                    tracingService.Trace($"Including quote detail {childRecord.Id} - product {productRef.Id} is in product family hierarchy");
                                }
                                else
                                {
                                    recordsFilteredOut++;
                                    tracingService.Trace($"Filtered out quote detail {childRecord.Id} - no productid found");
                                    continue;
                                }
                            }
                            else if (validProductIds != null && validProductIds.Count == 0)
                            {
                                // Product family specified but no descendants found - skip all records
                                recordsFilteredOut++;
                                tracingService.Trace($"Filtered out quote detail {childRecord.Id} - product family {mappingProductFamilyRef.Id} has no descendants");
                                continue;
                            }

                            // Record passed all filters - include in JSON
                            if (validProductIds == null)
                            {
                                recordsIncluded++; // No product family filter - include all
                            }

                            JObject rowObj = new JObject();
                            var visitedRow = new HashSet<Guid>();

                            foreach (var mapping in group)
                            {
                                ProcessMappingRecursiveForRow(service, tracingService, mapping, childRecord, rowObj, visitedRow, depth: 0);
                            }

                            tableRows.Add(rowObj);
                        }

                        // Log filtering results
                        tracingService.Trace($"=== Filtering results for '{heading}' ===");
                        tracingService.Trace($"Total records retrieved: {childRecords.Entities.Count}");
                        tracingService.Trace($"Records included in JSON: {recordsIncluded}");
                        tracingService.Trace($"Records filtered out: {recordsFilteredOut}");
                        if (mappingProductType != null && mappingProductType.Value == 826080004 && mappingProductFamilyRef != null)
                        {
                            tracingService.Trace($"Product family filter applied: {mappingProductFamilyRef.Id} with {validProductIds?.Count ?? 0} descendants");
                        }
                        tracingService.Trace($"Table rows in JSON: {tableRows.Count}");
                        tracingService.Trace($"========================================");

                        // Sorting logic...
                        var tableSection = documentJson["document"]?.Children<JObject>()
                            .FirstOrDefault(d => d["h1"]?.ToString() == heading);

                        if (tableSection != null)
                        {
                            var table = tableSection["children"]?.First?["table"];
                            var headers = table?["header"]?.ToObject<string[]>();
                            var sortOrders = table?["sort"]?.ToObject<string[]>();

                            if (headers != null && sortOrders != null && headers.Length == sortOrders.Length)
                            {
                                string sortColumn = null;
                                string sortOrder = null;

                                for (int i = 0; i < sortOrders.Length; i++)
                                {
                                    string orderLower = sortOrders[i].ToLower();
                                    if (orderLower == "asc" || orderLower == "dsc")
                                    {
                                        sortColumn = headers[i];
                                        sortOrder = orderLower;
                                        break;
                                    }
                                }

                                if (!string.IsNullOrWhiteSpace(sortColumn) && !string.IsNullOrWhiteSpace(sortOrder))
                                {
                                    tracingService.Trace($"Sorting table for '{heading}' on column '{sortColumn}' with order '{sortOrder}'");

                                    var headerToPlaceholder = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                                    foreach (var mapping in group)
                                    {
                                        string headerName = mapping.GetAttributeValue<string>("cm_columnheadernameindocument");
                                        string placeholder = mapping.GetAttributeValue<string>("cm_placeholdername");
                                        if (!string.IsNullOrWhiteSpace(headerName) && !string.IsNullOrWhiteSpace(placeholder))
                                        {
                                            headerToPlaceholder[headerName] = placeholder;
                                        }
                                    }

                                    if (headerToPlaceholder.TryGetValue(sortColumn, out string sortPlaceholder))
                                    {
                                        tracingService.Trace($"Found placeholder '{sortPlaceholder}' for sort column '{sortColumn}'");

                                        IOrderedEnumerable<JToken> sortedRows;
                                        if (sortOrder == "asc")
                                        {
                                            sortedRows = tableRows.OrderBy(row => ((JObject)row)[sortPlaceholder]?.ToString() ?? "zzz", StringComparer.OrdinalIgnoreCase);
                                        }
                                        else
                                        {
                                            sortedRows = tableRows.OrderByDescending(row => ((JObject)row)[sortPlaceholder]?.ToString() ?? "", StringComparer.OrdinalIgnoreCase);
                                        }

                                        tableRows = new JArray(sortedRows);
                                    }
                                }
                            }
                        }

                        mappingDataArray.Add(new JObject
                        {
                            ["heading"] = heading,
                            ["isTable"] = true,
                            ["placeholders"] = tableRows
                        });
                    }
                }

                context.OutputParameters["returnJson"] = mappingDataArray.ToString();
                tracingService.Trace("Returning mapping_data array successfully.");
            }
            catch (Exception ex)
            {
                tracingService.Trace("Exception: " + ex);
                throw new InvalidPluginExecutionException("Plugin Error: " + ex.Message);
            }
        }

        private void ProcessMappingRecursive(IOrganizationService service, ITracingService tracingService,
            Entity mapping, Entity currentRecord, JObject placeholders, HashSet<Guid> visited, int depth)
        {
            if (mapping == null || depth > MaxDepth || visited.Contains(mapping.Id)) return;
            visited.Add(mapping.Id);

            string placeholder = mapping.GetAttributeValue<string>("cm_placeholdername");
            string fieldSchemaName = mapping.GetAttributeValue<string>("cm_fieldschemaname");
            OptionSetValue fieldType = mapping.GetAttributeValue<OptionSetValue>("cm_fieldtype");

            bool mappingLineItem = mapping.GetAttributeValue<bool?>("cm_lineitem") ?? false;
            EntityCollection childMappings = RetrieveMappings(service, null, mapping.Id, mappingLineItem, tracingService);

            if (fieldType != null)
            {
                switch (fieldType.Value)
                {
                    case 121540000: // String
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName))
                            placeholders[placeholder] = currentRecord[fieldSchemaName]?.ToString();
                        break;

                    case 121540001: // OptionSet
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName) && currentRecord[fieldSchemaName] is OptionSetValue optionValue)
                            placeholders[placeholder] = currentRecord.FormattedValues.Contains(fieldSchemaName)
                                ? currentRecord.FormattedValues[fieldSchemaName]
                                : optionValue.Value.ToString();
                        break;

                    case 121540002: // Lookup
                        if (currentRecord.Contains(fieldSchemaName) && currentRecord[fieldSchemaName] is EntityReference lookupRef)
                        {
                            if (!string.IsNullOrWhiteSpace(placeholder))
                                placeholders[placeholder] = lookupRef.Name ?? lookupRef.Id.ToString();

                            Entity lookupRecord = null;
                            try { lookupRecord = service.Retrieve(lookupRef.LogicalName, lookupRef.Id, new ColumnSet(true)); }
                            catch (Exception ex) { tracingService.Trace($"Lookup retrieve failed: {ex.Message}"); }

                            if (lookupRecord != null)
                            {
                                foreach (var child in childMappings.Entities)
                                    ProcessMappingRecursive(service, tracingService, child, lookupRecord, placeholders, visited, depth + 1);
                                return;
                            }
                        }
                        break;

                    case 121540003: // DateTime
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName) && currentRecord[fieldSchemaName] is DateTime dt)
                            placeholders[placeholder] = dt.ToString("yyyy-MM-dd");
                        break;

                    case 121540004: // Decimal - Formatted to 2 decimal places
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName))
                            placeholders[placeholder] = currentRecord.GetAttributeValue<decimal?>(fieldSchemaName)?.ToString("F2") ?? "-";
                        break;

                    case 121540005: // Integer
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName))
                            placeholders[placeholder] = currentRecord.GetAttributeValue<int?>(fieldSchemaName)?.ToString() ?? "-";
                        break;

                    case 121540006: // Currency
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName) && currentRecord[fieldSchemaName] is Money moneyValue)
                        {
                            // Numeric value
                            placeholders[placeholder] = moneyValue.Value;

                            // Or formatted string
                            if (currentRecord.FormattedValues.Contains(fieldSchemaName))
                                placeholders[placeholder] = currentRecord.FormattedValues[fieldSchemaName];
                        }
                        break;
                }
            }

            foreach (var child in childMappings.Entities)
                ProcessMappingRecursive(service, tracingService, child, currentRecord, placeholders, visited, depth + 1);
        }

        private void ProcessMappingRecursiveForRow(IOrganizationService service, ITracingService tracingService,
            Entity mapping, Entity currentRecord, JObject placeholdersRow, HashSet<Guid> visited, int depth)
        {
            if (mapping == null || depth > MaxDepth || visited.Contains(mapping.Id)) return;
            visited.Add(mapping.Id);

            string placeholder = mapping.GetAttributeValue<string>("cm_placeholdername");
            string fieldSchemaName = mapping.GetAttributeValue<string>("cm_fieldschemaname");
            OptionSetValue fieldType = mapping.GetAttributeValue<OptionSetValue>("cm_fieldtype");

            bool mappingLineItem = mapping.GetAttributeValue<bool?>("cm_lineitem") ?? false;
            EntityCollection childMappings = RetrieveMappings(service, null, mapping.Id, mappingLineItem, tracingService);

            if (fieldType != null)
            {
                switch (fieldType.Value)
                {
                    case 121540000: // String
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName))
                            placeholdersRow[placeholder] = currentRecord[fieldSchemaName]?.ToString();
                        break;

                    case 121540001: // OptionSet
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName) && currentRecord[fieldSchemaName] is OptionSetValue optionValue)
                            placeholdersRow[placeholder] = currentRecord.FormattedValues.Contains(fieldSchemaName)
                                ? currentRecord.FormattedValues[fieldSchemaName]
                                : optionValue.Value.ToString();
                        break;

                    case 121540002: // Lookup
                        if (currentRecord.Contains(fieldSchemaName) && currentRecord[fieldSchemaName] is EntityReference lookupRef)
                        {
                            if (!string.IsNullOrWhiteSpace(placeholder))
                                placeholdersRow[placeholder] = lookupRef.Name ?? lookupRef.Id.ToString();

                            Entity lookupRecord = null;
                            try { lookupRecord = service.Retrieve(lookupRef.LogicalName, lookupRef.Id, new ColumnSet(true)); }
                            catch (Exception ex) { tracingService.Trace($"Lookup retrieve failed: {ex.Message}"); }

                            if (lookupRecord != null)
                            {
                                foreach (var child in childMappings.Entities)
                                    ProcessMappingRecursiveForRow(service, tracingService, child, lookupRecord, placeholdersRow, visited, depth + 1);
                                return;
                            }
                        }
                        break;

                    case 121540003: // DateTime
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName) && currentRecord[fieldSchemaName] is DateTime dt)
                            placeholdersRow[placeholder] = dt.ToString("yyyy-MM-dd");
                        break;

                    case 121540004: // Decimal - Formatted to 2 decimal places
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName))
                            placeholdersRow[placeholder] = currentRecord.GetAttributeValue<decimal?>(fieldSchemaName)?.ToString("F2") ?? "-";
                        break;

                    case 121540005: // Integer
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName))
                            placeholdersRow[placeholder] = currentRecord.GetAttributeValue<int?>(fieldSchemaName)?.ToString() ?? "-";
                        break;

                    case 121540006: // Currency
                        if (!string.IsNullOrWhiteSpace(placeholder) && currentRecord.Contains(fieldSchemaName) && currentRecord[fieldSchemaName] is Money moneyValue)
                        {
                            placeholdersRow[placeholder] = moneyValue.Value;

                            if (currentRecord.FormattedValues.Contains(fieldSchemaName))
                                placeholdersRow[placeholder] = currentRecord.FormattedValues[fieldSchemaName];
                        }
                        break;
                }
            }

            foreach (var child in childMappings.Entities)
                ProcessMappingRecursiveForRow(service, tracingService, child, currentRecord, placeholdersRow, visited, depth + 1);
        }

        private List<Guid> GetAllDescendantProducts(IOrganizationService service, Guid familyId, ITracingService tracing)
        {
            tracing.Trace($"Getting descendants for product family {familyId}");
            var descendants = new List<Guid>();
            var queue = new Queue<Guid>();
            queue.Enqueue(familyId);

            while (queue.Count > 0)
            {
                var current = queue.Dequeue();
                tracing.Trace($"Processing product {current} for children");

                // Get direct children where parentproductid = current
                var qe = new QueryExpression("product")
                {
                    ColumnSet = new ColumnSet("productid", "productstructure"),
                    Criteria = new FilterExpression(LogicalOperator.And)
                };
                qe.Criteria.AddCondition("parentproductid", ConditionOperator.Equal, current);
                qe.Criteria.AddCondition("statecode", ConditionOperator.Equal, 0);

                var results = service.RetrieveMultiple(qe).Entities;
                tracing.Trace($"Found {results.Count} direct children of {current}");

                foreach (var prod in results)
                {
                    var id = prod.Id;
                    var structureOption = prod.GetAttributeValue<OptionSetValue>("productstructure");
                    var structure = structureOption?.Value ?? 0;
                    tracing.Trace($"Child {id}: structure={structure}");

                    // Add all children (both product families and leaf products)
                    descendants.Add(id);
                    tracing.Trace($"Added product {id} to descendants");

                    // If it's a product family (structure = 2), get its children too (sub-children, level 2)
                    if (structure == 2)
                    {
                        queue.Enqueue(id);
                        tracing.Trace($"Queued product family {id} to get its children (sub-children)");
                    }
                }
            }

            tracing.Trace($"Total descendants from family {familyId}: {descendants.Count}");
            return descendants;
        }

        private EntityCollection RetrieveMappings(IOrganizationService service, Guid? templateGuid, Guid? parentId, bool lineItem, ITracingService tracingService)
        {
            var qe = new QueryExpression("cm_templatefieldmapping")
            {
                ColumnSet = new ColumnSet("cm_placeholdername", "cm_fieldschemaname", "cm_tablelogicalname", "cm_fieldtype", "cm_lineitem", "cm_tablesection", "cm_parententitylogicalnameonlineitem", "cm_columnheadernameindocument", "imd_producttype", "imd_productfamily"),
                Criteria = new FilterExpression(LogicalOperator.And)
            };

            if (templateGuid.HasValue)
            {
                qe.Criteria.AddCondition("cm_documenttemplate", ConditionOperator.Equal, templateGuid.Value);
                qe.Criteria.AddCondition("cm_parentmapping", ConditionOperator.Null);
            }

            if (parentId.HasValue)
            {
                qe.Criteria.AddCondition("cm_parentmapping", ConditionOperator.Equal, parentId.Value);
            }
            else
            {
                qe.Criteria.AddCondition("cm_parentmapping", ConditionOperator.Null);
            }

            qe.Criteria.AddCondition("cm_lineitem", ConditionOperator.Equal, lineItem);

            var result = service.RetrieveMultiple(qe);
            tracingService.Trace($"RetrieveMappings(template:{templateGuid}, parent:{parentId}, lineItem:{lineItem}) -> {result.Entities.Count} rows");
            return result;
        }
    }
}
