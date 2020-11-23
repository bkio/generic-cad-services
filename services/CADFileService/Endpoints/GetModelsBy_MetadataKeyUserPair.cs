/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Net;
using BCloudServiceUtilities;
using BWebServiceUtilities;
using CADFileService.Controllers;
using CADFileService.Endpoints.Structures;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using Newtonsoft.Json.Linq;

namespace CADFileService
{
    internal class GetModelsBy_MetadataKeyUserPair : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly string RestfulUrlParameter_UserID;
        private readonly string RestfulUrlParameter_MetadataKey;

        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        public GetModelsBy_MetadataKeyUserPair(IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_UserID, string _RestfulUrlParameter_MetadataKey)
        {
            DatabaseService = _DatabaseService;
            RestfulUrlParameter_UserID = _RestfulUrlParameter_UserID;
            RestfulUrlParameter_MetadataKey = _RestfulUrlParameter_MetadataKey;
        }

        public override BWebServiceResponse OnRequest_Interruptable_DeliveryEnsurerUser(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            GetTracingService()?.On_FromGatewayToService_Received(_Context, _ErrorMessageAction);

            var Result = OnRequest_Internal(_Context, _ErrorMessageAction);

            GetTracingService()?.On_FromServiceToGateway_Sent(_Context, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse OnRequest_Internal(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            AuthorizedUser = ServiceUtilities.Common.Methods.GetAuthorizedRequester(_Context, _ErrorMessageAction);
            if (!AuthorizedUser.bParseSuccessful) return BWebResponse.InternalError(AuthorizedUser.ParseErrorMessage);

            if (_Context.Request.HttpMethod != "GET")
            {
                _ErrorMessageAction?.Invoke("GetModelsBy_MetadataKeyValueUserPair: GET method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("GET method is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            var UserID = RestfulUrlParameters[RestfulUrlParameter_UserID];
            var MetadataKey = WebUtility.UrlDecode(RestfulUrlParameters[RestfulUrlParameter_MetadataKey]);

            if (!DatabaseService.GetItem(
                AttributeKUPairToOwnerDBEntry.TABLE(),
                AttributeKUPairToOwnerDBEntry.KEY_NAME,
                Controller_AttributeTables.MakeKey(MetadataKey, UserID),
                new string[] { AttributeKeyDBEntryBase.METADATA_LOCATOR_PROPERTY },
                out JObject Result,
                _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Database query has failed.");
            }

            if (Result == null || !Result.ContainsKey(AttributeKeyDBEntryBase.METADATA_LOCATOR_PROPERTY) || Result[AttributeKeyDBEntryBase.METADATA_LOCATOR_PROPERTY].Type != JTokenType.Array)
            {
                return BWebResponse.NotFound("Entry not found.");
            }

            var AsArray = (JArray)Result[AttributeKeyDBEntryBase.METADATA_LOCATOR_PROPERTY];
            for (int i = 0; i < AsArray.Count; i++)
            {
                var AsStr = (string)AsArray[i];
                if (AsStr != null)
                {
                    if (AsStr.StartsWith(Controller_AttributeTables.MODEL_METADATA_PREFIX))
                    {
                        AsStr = AsStr.Substring(Controller_AttributeTables.MODEL_METADATA_PREFIX.Length);
                    }
                    else if (AsStr.StartsWith(Controller_AttributeTables.REVISION_METADATA_PREFIX))
                    {
                        AsStr = AsStr.Substring(Controller_AttributeTables.REVISION_METADATA_PREFIX.Length);
                        AsStr = AsStr.Replace(Controller_AttributeTables.REVISION_METADATA_MRV_DELIMITER, "->");
                    }

                    AsArray[i] = AsStr;
                }
            }

            return BWebResponse.StatusOK("Model(s) have been located.", new JObject()
            {
                [AttributeKeyDBEntryBase.METADATA_LOCATOR_PROPERTY] = AsArray
            });
        }
    }
}