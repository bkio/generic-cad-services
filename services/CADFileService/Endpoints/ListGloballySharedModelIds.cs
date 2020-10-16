/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Net;
using BCloudServiceUtilities;
using BWebServiceUtilities;
using CADFileService.Endpoints.Structures;
using ServiceUtilities.All;
using Newtonsoft.Json.Linq;

namespace CADFileService.Endpoints
{
    internal class ListGloballySharedModelIds
    {
        public class ForInternal : InternalWebServiceBase
        {
            private readonly IBDatabaseServiceInterface DatabaseService;

            public ForInternal(string _InternalCallPrivateKey, IBDatabaseServiceInterface _DatabaseService) : base(_InternalCallPrivateKey)
            {
                DatabaseService = _DatabaseService;
            }

            protected override BWebServiceResponse Process(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
            {
                return ProcessCommon(_Context, DatabaseService, _ErrorMessageAction);
            }
        }

        internal class ForUsers : BppWebServiceBase
        {
            private readonly IBDatabaseServiceInterface DatabaseService;

            public ForUsers(IBDatabaseServiceInterface _DatabaseService)
            {
                DatabaseService = _DatabaseService;
            }

            protected override BWebServiceResponse OnRequestPP(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
            {
                return ProcessCommon(_Context, DatabaseService, _ErrorMessageAction);
            }
        }

        public static BWebServiceResponse ProcessCommon(HttpListenerContext _Context, IBDatabaseServiceInterface _DatabaseService, Action<string> _ErrorMessageAction = null)
        {
            if (_Context.Request.HttpMethod != "GET")
            {
                _ErrorMessageAction?.Invoke("ListGloballySharedModelIds: GET method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("GET method is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            if (!_DatabaseService.ScanTable(GloballySharedModelIDsDBEntry.DBSERVICE_GLOBALLY_SHARED_MODEL_IDS_TABLE(), out List<JObject> GloballySharedModelIDObjects, _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Scanning table for listing items has failed.");
            }

            var SharedModelIds = new JArray();
            foreach (var Current in GloballySharedModelIDObjects)
            {
                if (Current != null && Current.ContainsKey(GloballySharedModelIDsDBEntry.KEY_NAME_MODEL_ID))
                {
                    SharedModelIds.Add((string)Current[GloballySharedModelIDsDBEntry.KEY_NAME_MODEL_ID]);
                }
            }

            return BWebResponse.StatusOK("Globally shared models have successfully been retrieved.", new JObject()
            {
                ["sharedModelIds"] = SharedModelIds
            });
        }
    }
}