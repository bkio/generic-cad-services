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

namespace CADFileService.Endpoints
{
    class Model_StopCurrentProcess : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBFileServiceInterface FileService;
        private readonly IBDatabaseServiceInterface DatabaseService;

        private readonly string RestfulUrlParameter_ModelsKey;
        private readonly string RestfulUrlParameter_RevisionsKey;

        private string RequestedModelID;
        private int RequestedRevisionIndex;

        private AuthorizedRequester AuthorizedUser;

        private readonly string CadProcessServiceEndpoint;

        public Model_StopCurrentProcess(IBFileServiceInterface _FileService, IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey, string _RestfulUrlParameter_RevisionsKey, string _CadProcessServiceEndpoint)
        {
            FileService = _FileService;
            DatabaseService = _DatabaseService;
            RestfulUrlParameter_ModelsKey = _RestfulUrlParameter_ModelsKey;
            RestfulUrlParameter_RevisionsKey = _RestfulUrlParameter_RevisionsKey;
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

            if (_Context.Request.HttpMethod != "GET")
            {
                _ErrorMessageAction?.Invoke("Model_StopCurrentProcess: GET method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("GET method is accepted. But received request method: " + _Context.Request.HttpMethod);
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

            BWebServiceResponse Result = StopCurrentProcess(_Context, _ErrorMessageAction);

            Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse StopCurrentProcess(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
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

            //if (RevisionObject.FileEntry.FileUploadProcessStage == (int)EUploadProcessStage.NotUploaded)
            //{
            //    return BWebResponse.StatusAccepted("File has not been uploaded yet.");
            //}

            //if (RevisionObject.FileEntry.FileUploadProcessStage == (int)EUploadProcessStage.Uploaded_Processed)
            //{
            //    return BWebResponse.StatusAccepted("File has been already processed.");
            //}

            //if (RevisionObject.FileEntry.FileUploadProcessStage == (int)EUploadProcessStage.Uploaded_ProcessFailed)
            //{
            //    return BWebResponse.StatusAccepted("File process has been already failed.");
            //}

            RevisionObject.FileEntry.FileUploadProcessStage = (int)EUploadProcessStage.Uploaded_ProcessFailed;
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

            if (!SendStopProcessRequest(_Context, ModelObject.ModelName, RevisionObject.RevisionIndex, _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Stop request sending has been failed.");
            }

            return BWebResponse.StatusAccepted("Stop request has been sent.");
        }

        private bool SendStopProcessRequest(
            HttpListenerContext _Context,
            string _ModelUniqueName,
            int RevisionIndex,
            Action<string> _ErrorMessageAction)
        {
            var RequestObject = new JObject()
            {
                ["processMode"] = (int)EProcessMode.VirtualMachine,
                ["modelUniqueName"] = _ModelUniqueName,
                ["revisionIndex"] = RevisionIndex
            };

            int TryCount = 0;
            BWebServiceExtraUtilities.InterServicesRequestResponse Result;
            do
            {
                GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

                Result = BWebServiceExtraUtilities.InterServicesRequest(new BWebServiceExtraUtilities.InterServicesRequestRequest()
                {
                    DestinationServiceUrl = CadProcessServiceEndpoint + "3d/process/stop",
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
                _ErrorMessageAction?.Invoke("Error: SendStopProcessRequest: InterServicesRequest to stop processing has failed. Error: Code: " + Result.ResponseCode + ", Content: " + Result.Content.String);
                return false; //Retry
            }

            return true;
        }

        private static bool SleepReturnTrue(int _MS) { Thread.Sleep(_MS); return true; }

    }
}