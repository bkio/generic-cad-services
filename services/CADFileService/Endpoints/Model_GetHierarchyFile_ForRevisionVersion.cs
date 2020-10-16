/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License
using System;
using System.Net;
using BCloudServiceUtilities;
using BWebServiceUtilities;
using CADFileService.Endpoints.Common;
using ServiceUtilities.Process.RandomAccessFile;
using ServiceUtilities;
using ServiceUtilities.PubSubUsers.PubSubRelated;

namespace CADFileService
{
    internal class Model_GetHierarchyFile_ForRevisionVersion : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBFileServiceInterface FileService;
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly string CadFileStorageBucketName;

        private readonly string RestfulUrlParameter_ModelsKey;
        private readonly string RestfulUrlParameter_RevisionsKey;
        private readonly string RestfulUrlParameter_VersionsKey;

        private string RequestedModelID;
        private int RequestedRevisionIndex;
        private int RequestedVersionIndex;

        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        public Model_GetHierarchyFile_ForRevisionVersion(IBFileServiceInterface _FileService, IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey, string _RestfulUrlParameter_RevisionsKey, string _RestfulUrlParameter_VersionsKey, string _CadFileStorageBucketName)
        {
            FileService = _FileService;
            DatabaseService = _DatabaseService;
            CadFileStorageBucketName = _CadFileStorageBucketName;
            RestfulUrlParameter_ModelsKey = _RestfulUrlParameter_ModelsKey;
            RestfulUrlParameter_RevisionsKey = _RestfulUrlParameter_RevisionsKey;
            RestfulUrlParameter_VersionsKey = _RestfulUrlParameter_VersionsKey;
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
                _ErrorMessageAction?.Invoke("Model_GetHierarchyFile_ForRevisionVersion: GET method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("GET method is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            RequestedModelID = RestfulUrlParameters[RestfulUrlParameter_ModelsKey];
            if (!int.TryParse(RestfulUrlParameters[RestfulUrlParameter_RevisionsKey], out RequestedRevisionIndex))
            {
                return BWebResponse.BadRequest("Revision index must be an integer.");
            }
            if (!int.TryParse(RestfulUrlParameters[RestfulUrlParameter_VersionsKey], out RequestedVersionIndex))
            {
                return BWebResponse.BadRequest("Version index must be an integer.");
            }

            return GetProcessedHierarchyFile(_ErrorMessageAction);
        }

        private BWebServiceResponse GetProcessedHierarchyFile(Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.GetProcessedFile(
                this,
                ENodeType.Hierarchy,
                DatabaseService,
                FileService,
                CadFileStorageBucketName,
                RequestedModelID,
                RequestedRevisionIndex,
                RequestedVersionIndex,
                out BWebServiceResponse _SuccessResponse,
                out BWebServiceResponse _FailureResponse,
                _ErrorMessageAction))
            {
                return _FailureResponse;
            }
            return _SuccessResponse;
        }
    }
}