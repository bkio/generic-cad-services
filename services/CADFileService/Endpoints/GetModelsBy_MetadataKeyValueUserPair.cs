﻿/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADFileService.Controllers;
using CADFileService.Endpoints.Structures;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using Newtonsoft.Json.Linq;

namespace CADFileService
{
    internal class GetModelsBy_MetadataKeyValueUserPair : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;

        private readonly string RestfulUrlParameter_UserID;
        private readonly string RestfulUrlParameter_MetadataKey;
        private readonly string RestfulUrlParameter_MetadataValues;

        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        public GetModelsBy_MetadataKeyValueUserPair(IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_UserID, string _RestfulUrlParameter_MetadataKey, string _RestfulUrlParameter_MetadataValues)
        {
            DatabaseService = _DatabaseService;
            RestfulUrlParameter_UserID = _RestfulUrlParameter_UserID;
            RestfulUrlParameter_MetadataKey = _RestfulUrlParameter_MetadataKey;
            RestfulUrlParameter_MetadataValues = _RestfulUrlParameter_MetadataValues;
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
            var MetadataValues = WebUtility.UrlDecode(RestfulUrlParameters[RestfulUrlParameter_MetadataValues]);
            var SplittedValues = MetadataValues.Split("__DELIM__", StringSplitOptions.RemoveEmptyEntries);
            if (SplittedValues == null || SplittedValues.Length == 0)
            {
                return BWebResponse.BadRequest("Metadata values parameter is invalid. Metadata values must be separated by __DELIM__ delimiter.");
            }

            if (!DatabaseService.GetItem(
                AttributeKVPairUserToOwnerDBEntry.TABLE(),
                AttributeKVPairUserToOwnerDBEntry.KEY_NAME,
                Controller_AttributeTables.MakeKey(MetadataKey, new List<string>(SplittedValues), UserID),
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