﻿/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADFileService.Endpoints.Common;
using CADFileService.Endpoints.Structures;
using ServiceUtilities.All;
using ServiceUtilities.Process.Procedure;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using Newtonsoft.Json.Linq;

namespace CADFileService.Endpoints
{
    class Model_GetUnrealGeometry : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBFileServiceInterface FileService;
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly string CadFileStorageBucketName;

        private readonly string RestfulUrlParameter_ModelsKey;
        private readonly string RestfulUrlParameter_RevisionsKey;
        private readonly string RestfulUrlParameter_GeometryKey;

        private string RequestedModelID;
        private int RequestedRevisionIndex;
        private string RequestedGeometryId;

        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        public Model_GetUnrealGeometry(IBFileServiceInterface _FileService, IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey, string _RestfulUrlParameter_RevisionsKey, string _RestfulUrlParameter_GeometryKey, string _CadFileStorageBucketName)
        {
            FileService = _FileService;
            DatabaseService = _DatabaseService;
            CadFileStorageBucketName = _CadFileStorageBucketName;
            RestfulUrlParameter_ModelsKey = _RestfulUrlParameter_ModelsKey;
            RestfulUrlParameter_RevisionsKey = _RestfulUrlParameter_RevisionsKey;
            RestfulUrlParameter_GeometryKey = _RestfulUrlParameter_GeometryKey;
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
                _ErrorMessageAction?.Invoke("Model_GetHierarchyFile_ForRevision: GET method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("GET method is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            string RequestedModelName_UrlEncoded = WebUtility.UrlEncode(RestfulUrlParameters[RestfulUrlParameter_ModelsKey]);

            if (!DatabaseService.GetItem(
                    UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(),
                    UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME,
                    new BPrimitiveType(RequestedModelName_UrlEncoded),
                    UniqueFileFieldsDBEntry.Properties,
                    out JObject ModelIDResponse,
                    _ErrorMessageAction) || !ModelIDResponse.ContainsKey(ModelDBEntry.KEY_NAME_MODEL_ID))
            {
                return BWebResponse.InternalError("Model ID could not be retrieved upon conflict.");
            }

            RequestedModelID = (string)ModelIDResponse[ModelDBEntry.KEY_NAME_MODEL_ID];

            if (!int.TryParse(RestfulUrlParameters[RestfulUrlParameter_RevisionsKey], out RequestedRevisionIndex))
            {
                return BWebResponse.BadRequest("Revision index must be an integer.");
            }

            RequestedGeometryId = RestfulUrlParameters[RestfulUrlParameter_GeometryKey];

            return GetProcessedUnrealHierarchyGeometryFile(RequestedGeometryId, _ErrorMessageAction);
        }

        private BWebServiceResponse GetProcessedUnrealHierarchyGeometryFile(string _GeometryId, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.GetProcessedFile(
                this,
                EProcessedFileType.UNREAL_G,
                DatabaseService,
                FileService,
                CadFileStorageBucketName,
                RequestedModelID,
                RequestedRevisionIndex,
                out BWebServiceResponse _SuccessResponse,
                out BWebServiceResponse _FailureResponse,
                _GeometryId,
                _ErrorMessageAction))
            {
                return _FailureResponse;
            }
            return _SuccessResponse;
        }
    }
}
