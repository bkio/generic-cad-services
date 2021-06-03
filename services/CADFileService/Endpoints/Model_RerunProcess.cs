/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADFileService.Controllers;
using CADFileService.Endpoints.Common;
using CADFileService.Endpoints.Structures;
using ServiceUtilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using ServiceUtilities.Common;
using System.Threading;
using System.IO;

namespace CADFileService.Endpoints
{
    class Model_RerunProcess : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBFileServiceInterface FileService;
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly string CadFileStorageBucketName;

        private readonly string RestfulUrlParameter_ModelsKey;
        private readonly string RestfulUrlParameter_RevisionsKey;

        private string RequestedModelID;
        private int RequestedRevisionIndex;

        private AuthorizedRequester AuthorizedUser;

        private readonly string CadProcessServiceEndpoint;

        private int ProcessStage = -1;

        public Model_RerunProcess(IBFileServiceInterface _FileService, IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey, string _RestfulUrlParameter_RevisionsKey, string _CadFileStorageBucketName, string _CadProcessServiceEndpoint)
        {
            FileService = _FileService;
            DatabaseService = _DatabaseService;
            RestfulUrlParameter_ModelsKey = _RestfulUrlParameter_ModelsKey;
            RestfulUrlParameter_RevisionsKey = _RestfulUrlParameter_RevisionsKey;
            CadFileStorageBucketName = _CadFileStorageBucketName;
            CadProcessServiceEndpoint = _CadProcessServiceEndpoint;
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
            AuthorizedUser = Methods.GetAuthorizedRequester(_Context, _ErrorMessageAction);
            if (!AuthorizedUser.bParseSuccessful) return BWebResponse.InternalError(AuthorizedUser.ParseErrorMessage);

            if (_Context.Request.HttpMethod != "POST")
            {
                _ErrorMessageAction?.Invoke("Model_RerunProcess: POST method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("POST method is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            var RequestedModelName = WebUtility.UrlDecode(RestfulUrlParameters[RestfulUrlParameter_ModelsKey]);

            if (!CommonMethods.TryGettingModelID(
                DatabaseService,
                RequestedModelName,
                out RequestedModelID,
                out BWebServiceResponse FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
            }

            if (!int.TryParse(RestfulUrlParameters[RestfulUrlParameter_RevisionsKey], out RequestedRevisionIndex))
            {
                return BWebResponse.BadRequest("Revision index must be an integer.");
            }

            if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Atomic operation control has failed.");
            }

            using (var InputStream = _Context.Request.InputStream)
            {
                using (var ResponseReader = new StreamReader(InputStream))
                {
                    try
                    {
                        var ParsedBody = JObject.Parse(ResponseReader.ReadToEnd());

                        if (ParsedBody.ContainsKey(FileEntry.CURRENT_PROCESS_STAGE_PROPERTY))
                        {
                            var ProcessStageToken = ParsedBody[FileEntry.CURRENT_PROCESS_STAGE_PROPERTY];
                            if (ProcessStageToken.Type != JTokenType.Integer)
                            {
                                return BWebResponse.BadRequest("Request body contains invalid fields.");
                            }
                            ProcessStage = (int)ProcessStageToken;
                        }
                    }
                    catch (Exception e)
                    {
                        _ErrorMessageAction?.Invoke("Model_RerunProcess: Read request body stage has failed. Exception: " + e.Message + ", Trace: " + e.StackTrace);
                        return BWebResponse.BadRequest("Malformed request body. Request must be a valid json form.");
                    }
                }
            }

            BWebServiceResponse Result = RerunCurrentProcess(_Context, _ErrorMessageAction);

            Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse RerunCurrentProcess(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryGettingAllInfo(
                DatabaseService,
                RequestedModelID,
                RequestedRevisionIndex,
                out ModelDBEntry ModelObject,
                out Revision RevisionObject,
                out int _,
                out BWebServiceResponse FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
            }

            if (RevisionObject.FileEntry.FileUploadProcessStage == (int)EUploadProcessStage.NotUploaded)
            {
                return BWebResponse.StatusAccepted("File has not been uploaded yet.");
            }

            if (ProcessStage < 0)
            {
                RevisionObject.FileEntry.CurrentProcessStage = (int)EProcessStage.Stage0_FileUpload;
            }

            RevisionObject.FileEntry.FileUploadProcessStage = (int)EUploadProcessStage.Uploaded_Processing;
            RevisionObject.FileEntry.FileRelativeUrl = RevisionObject.FileEntry.GetFileRelativeUrl(ModelObject.ModelName, RevisionObject.RevisionIndex);
            RevisionObject.FileEntry.FileProcessedAtTime = Methods.ToISOString();
            ModelObject.MRVLastUpdateTime = RevisionObject.FileEntry.FileProcessedAtTime;

            Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                new BPrimitiveType(RequestedModelID),
                JObject.Parse(JsonConvert.SerializeObject(ModelObject)));

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionFileEntryUpdated
            (
                RequestedModelID,
                RequestedRevisionIndex,
                ModelObject.ModelOwnerUserID,
                ModelObject.ModelSharedWithUserIDs,
                AuthorizedUser.UserID,
                JObject.Parse(JsonConvert.SerializeObject(RevisionObject.FileEntry))
            ),
            _ErrorMessageAction);

            if (!SendStartProcessRequest(_Context, ModelObject.ModelName, RevisionObject, _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Start request sending has been failed.");
            }

            return BWebResponse.StatusAccepted("Start request has been sent.");
        }

        private bool SendStartProcessRequest(
            HttpListenerContext _Context,
            string _ModelUniqueName,
            Revision _RevisionObject,
            Action<string> _ErrorMessageAction)
        {

            string ZipMainAssembly = "";

            if (_RevisionObject.FileEntry.ZipTypeMainAssemblyFileNameIfAny != null)
            {
                ZipMainAssembly = _RevisionObject.FileEntry.ZipTypeMainAssemblyFileNameIfAny;
            }

            var RequestObject = new JObject()
            {
                ["bucketName"] = CadFileStorageBucketName,
                ["rawFileRelativeUrl"] = _RevisionObject.FileEntry.FileRelativeUrl,
                ["modelName"] = _ModelUniqueName,
                ["modelRevision"] = _RevisionObject.RevisionIndex,
                ["zipTypeMainAssemblyFileNameIfAny"] = ZipMainAssembly,
                ["processStep"] = _RevisionObject.FileEntry.CurrentProcessStage,
                ["filters"] = JsonConvert.SerializeObject(_RevisionObject.FileEntry.Layers),
                ["globalScale"] = _RevisionObject.FileEntry.GlobalTransformOffset.UniformScale,
                ["globalXOffset"] = _RevisionObject.FileEntry.GlobalTransformOffset.LocationOffsetX,
                ["globalYOffset"] = _RevisionObject.FileEntry.GlobalTransformOffset.LocationOffsetY,
                ["globalZOffset"] = _RevisionObject.FileEntry.GlobalTransformOffset.LocationOffsetZ,
                ["globalXRotation"] = _RevisionObject.FileEntry.GlobalTransformOffset.RotationOffsetX,
                ["globalYRotation"] = _RevisionObject.FileEntry.GlobalTransformOffset.RotationOffsetY,
                ["globalZRotation"] = _RevisionObject.FileEntry.GlobalTransformOffset.RotationOffsetZ,
                ["optimizationPreset"] = _RevisionObject.FileEntry.OptimizationPreset,
                ["mergeFinalLevel"] = _RevisionObject.FileEntry.bMergeFinalLevel,
                ["deleteDuplicates"] = _RevisionObject.FileEntry.bDetectDuplicateMeshes
            };

            int TryCount = 0;
            BWebServiceExtraUtilities.InterServicesRequestResponse Result;
            do
            {
                GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

                Result = BWebServiceExtraUtilities.InterServicesRequest(new BWebServiceExtraUtilities.InterServicesRequestRequest()
                {
                    DestinationServiceUrl = CadProcessServiceEndpoint + "3d/process/start",
                    RequestMethod = "POST",
                    ContentType = "application/json",
                    Content = new BStringOrStream(RequestObject.ToString()),
                    bWithAuthToken = false, //Kubernetes Service
                    UseContextHeaders = _Context,
                    ExcludeHeaderKeysForRequest = null
                },
                false,
                _ErrorMessageAction);

                GetTracingService()?.On_FromServiceToService_Received(_Context, _ErrorMessageAction);

            } while (++TryCount < 10 && !Result.bSuccess && SleepReturnTrue(1000));

            if (!Result.bSuccess || Result.ResponseCode >= 400)
            {
                _ErrorMessageAction?.Invoke("Error: SendStartProcessRequest: InterServicesRequest to start processing has failed. Error: Code: " + Result.ResponseCode + ", Content: " + Result.Content.String);
                return false; //Retry
            }

            return true;
        }

        private static bool SleepReturnTrue(int _MS) { Thread.Sleep(_MS); return true; }

    }
}
