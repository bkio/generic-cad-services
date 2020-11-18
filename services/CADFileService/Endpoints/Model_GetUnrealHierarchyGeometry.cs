using BCloudServiceUtilities;
using BWebServiceUtilities;
using CADFileService.Endpoints.Common;
using ServiceUtilities.Process.Procedure;
using ServiceUtilities.All;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace CADFileService.Endpoints
{
    class Model_GetUnrealHierarchyGeometry : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBFileServiceInterface FileService;
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly string CadFileStorageBucketName;

        private readonly string RestfulUrlParameter_ModelsKey;
        private readonly string RestfulUrlParameter_RevisionsKey;

        private string RequestedModelID;
        private int RequestedRevisionIndex;

        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        public Model_GetUnrealHierarchyGeometry(IBFileServiceInterface _FileService, IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey, string _RestfulUrlParameter_RevisionsKey, string _CadFileStorageBucketName)
        {
            FileService = _FileService;
            DatabaseService = _DatabaseService;
            CadFileStorageBucketName = _CadFileStorageBucketName;
            RestfulUrlParameter_ModelsKey = _RestfulUrlParameter_ModelsKey;
            RestfulUrlParameter_RevisionsKey = _RestfulUrlParameter_RevisionsKey;
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

            RequestedModelID = RestfulUrlParameters[RestfulUrlParameter_ModelsKey];
            if (!int.TryParse(RestfulUrlParameters[RestfulUrlParameter_RevisionsKey], out RequestedRevisionIndex))
            {
                return BWebResponse.BadRequest("Revision index must be an integer.");
            }

            return GetProcessedUnrealHierarchyGeometryFile(_ErrorMessageAction);
        }

        private BWebServiceResponse GetProcessedUnrealHierarchyGeometryFile(Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.GetProcessedFile(
                this,
                EProcessedFileType.UNREAL_HG,
                DatabaseService,
                FileService,
                CadFileStorageBucketName,
                RequestedModelID,
                RequestedRevisionIndex,
                out BWebServiceResponse _SuccessResponse,
                out BWebServiceResponse _FailureResponse,
                null,
                _ErrorMessageAction))
            {
                return _FailureResponse;
            }
            return _SuccessResponse;
        }
    }
}
