/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using BWebServiceUtilities_GC;
using CADFileService.Controllers;
using CADFileService.Endpoints.Common;
using ServiceUtilities.Process.Procedure;
using CADFileService.Endpoints.Structures;
using ServiceUtilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceUtilities.PubSubUsers.PubSubRelated;

namespace CADFileService
{
    internal class Model_GetUpdateDeleteRaw_ForRevisionVersion : WebServiceBaseTimeoutableDeliveryEnsurerUser
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

        private readonly string CadProcessServiceEndpoint;

        public Model_GetUpdateDeleteRaw_ForRevisionVersion(IBFileServiceInterface _FileService, IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey, string _RestfulUrlParameter_RevisionsKey, string _RestfulUrlParameter_VersionsKey, string _CadFileStorageBucketName, string _CadProcessServiceEndpoint)
        {
            FileService = _FileService;
            DatabaseService = _DatabaseService;
            CadFileStorageBucketName = _CadFileStorageBucketName;
            RestfulUrlParameter_ModelsKey = _RestfulUrlParameter_ModelsKey;
            RestfulUrlParameter_RevisionsKey = _RestfulUrlParameter_RevisionsKey;
            RestfulUrlParameter_VersionsKey = _RestfulUrlParameter_VersionsKey;
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
            AuthorizedUser = ServiceUtilities.Common.Methods.GetAuthorizedRequester(_Context, _ErrorMessageAction);
            if (!AuthorizedUser.bParseSuccessful) return BWebResponse.InternalError(AuthorizedUser.ParseErrorMessage);

            if (_Context.Request.HttpMethod != "GET" && _Context.Request.HttpMethod != "POST" && _Context.Request.HttpMethod != "DELETE")
            {
                _ErrorMessageAction?.Invoke("Model_GetUpdateDeleteRaw_ForRevisionVersion: GET, POST and DELETE methods are accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("GET, POST and DELETE methods are accepted. But received request method: " + _Context.Request.HttpMethod);
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

            if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Atomic operation control has failed.");
            }

            BWebServiceResponse Result;

            if (_Context.Request.HttpMethod == "GET")
            {
                Result = GetRawFile(_ErrorMessageAction);
            }
            else
            {
                if (_Context.Request.HttpMethod == "DELETE")
                {
                    Result = DeleteRawFile(_Context, _ErrorMessageAction);
                }
                else
                {
                    Result = UpdateRawFile(_Context, _ErrorMessageAction);
                }
            }
            Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse UpdateRawFile(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryParsingRequestFor(
                _Context,
                out JObject UpdatedFileEntryJson,
                true, out FileEntry CheckForGenerateUploadUrl,
                out BWebServiceResponse FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
            }
            bool bGenerateUploadUrl = CheckForGenerateUploadUrl.bGenerateUploadUrl;
            if (bGenerateUploadUrl)
            {
                UpdatedFileEntryJson[FileEntry.FILE_ENTRY_FILE_TYPE_PROPERTY] = CheckForGenerateUploadUrl.FileEntryFileType.TrimStart('.').ToLower();
            }
            else
            {
                if (UpdatedFileEntryJson.ContainsKey(FileEntry.FILE_ENTRY_NAME_PROPERTY)
                    || UpdatedFileEntryJson.ContainsKey(FileEntry.FILE_ENTRY_FILE_TYPE_PROPERTY)
                    || UpdatedFileEntryJson.ContainsKey(FileEntry.ZIP_MAIN_ASSEMBLY_FILE_NAME_IF_ANY_PROPERTY)
                    || UpdatedFileEntryJson.ContainsKey(FileEntry.DATA_SOURCE_PROPERTY))
                {
                    return BWebResponse.BadRequest("Name|type related fields can only be set when " + FileEntry.GENERATE_UPLOAD_URL_PROPERTY + " option is true.");
                }
            }

            if (!CommonMethods.TryGettingAllInfo(
                DatabaseService,
                RequestedModelID,
                RequestedRevisionIndex,
                RequestedVersionIndex,
                out ModelDBEntry ModelObject,
                out Revision RevisionObject,
                out RevisionVersion VersionObject,
                out int _,
                out FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
            }

            if (VersionObject.FileEntry.FileEntryName != null && VersionObject.FileEntry.FileEntryName.Length > 0)
            {
                if (UpdatedFileEntryJson.ContainsKey(FileEntry.FILE_ENTRY_NAME_PROPERTY)
                    || UpdatedFileEntryJson.ContainsKey(FileEntry.FILE_ENTRY_FILE_TYPE_PROPERTY)
                    || UpdatedFileEntryJson.ContainsKey(FileEntry.ZIP_MAIN_ASSEMBLY_FILE_NAME_IF_ANY_PROPERTY)
                    || UpdatedFileEntryJson.ContainsKey(FileEntry.DATA_SOURCE_PROPERTY))
                {
                    return BWebResponse.BadRequest("File entry (raw) must be deleted before updating.");
                }
            }

            VersionObject.FileEntry.Merge(UpdatedFileEntryJson);

            VersionObject.FileEntry.FileEntryFileType = VersionObject.FileEntry.FileEntryFileType.ToLower().TrimStart('.');
            VersionObject.FileEntry.SetRelativeUrls_GetCommonUrlPart_FileEntryFileTypePreSet(RequestedModelID, RequestedRevisionIndex, RequestedVersionIndex);
            VersionObject.FileEntry.FileEntryCreationTime = CommonMethods.GetTimeAsCreationTime();
            ModelObject.MRVLastUpdateTime = VersionObject.FileEntry.FileEntryCreationTime;

            string UploadUrl_IfRequested = null;

            if (bGenerateUploadUrl &&

                !FileService.CreateSignedURLForUpload(
                    out UploadUrl_IfRequested,
                    CadFileStorageBucketName,
                    VersionObject.FileEntry.RawFileRelativeUrl,
                    FileEntry.RAW_FILE_UPLOAD_CONTENT_TYPE,
                    FileEntry.EXPIRY_MINUTES,
                    _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Signed url generation has failed.");
            }

            Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                new BPrimitiveType(RequestedModelID),
                JObject.Parse(JsonConvert.SerializeObject(ModelObject)));

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionVersionFileEntryUpdated
            (
                RequestedModelID,
                RequestedRevisionIndex,
                RequestedVersionIndex,
                ModelObject.ModelOwnerUserID,
                ModelObject.ModelSharedWithUserIDs,
                AuthorizedUser.UserID,
                UpdatedFileEntryJson
            ),
            _ErrorMessageAction);

            var ResultObject = new JObject();
            if (bGenerateUploadUrl)
            {
                ResultObject[FileEntry.FILE_UPLOAD_URL_PROPERTY] = UploadUrl_IfRequested;
                ResultObject[FileEntry.FILE_UPLOAD_CONTENT_TYPE_PROPERTY] = FileEntry.RAW_FILE_UPLOAD_CONTENT_TYPE;
            }

            return BWebResponse.StatusAccepted("Update raw file request has been accepted.", ResultObject);
        }

        private BWebServiceResponse DeleteRawFile(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryGettingAllInfo(
                   DatabaseService,
                   RequestedModelID,
                   RequestedRevisionIndex,
                   RequestedVersionIndex,
                   out ModelDBEntry ModelObject,
                   out Revision _,
                   out RevisionVersion VersionObject,
                   out int ModelRevisionVersionListIx,
                   out BWebServiceResponse _FailureResponse,
                   _ErrorMessageAction))
            {
                return _FailureResponse;
            }

            //if (VersionObject.FileEntry.FileProcessStage == (int)Constants.EProcessStage.Uploaded_Processing)
            //{
            //    var RequestObject = new JObject()
            //    {
            //        ["bucketName"] = CadFileStorageBucketName,
            //        ["rawFileRelativeUrl"] = VersionObject.FileEntry.RawFileRelativeUrl
            //    };

            //    GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

            //    var Result = BWebUtilities_GC_CloudRun.InterServicesRequest(new BWebUtilities_GC_CloudRun.InterServicesRequestRequest()
            //    {
            //        DestinationServiceUrl = CadProcessServiceEndpoint + "/process/stop",
            //        RequestMethod = "POST",
            //        ContentType = "application/json",
            //        Content = new BStringOrStream(RequestObject.ToString()),
            //        bWithAuthToken = false, //Kubernetes Service
            //        UseContextHeaders = _Context,
            //        ExcludeHeaderKeysForRequest = null
            //    },
            //    false,
            //    _ErrorMessageAction);

            //    GetTracingService()?.On_FromServiceToService_Received(_Context, _ErrorMessageAction);

            //    if (!Result.bSuccess || Result.ResponseCode >= 400)
            //    {
            //        return new BWebServiceResponse(Result.ResponseCode, Result.Content, Result.ContentType);
            //    }
            //}

            var PreviousProcessStage = VersionObject.FileEntry.FileProcessStage;
            if (PreviousProcessStage != (int)Constants.EProcessStage.NotUploaded)
            {
                Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionVersionFileEntryDeleteAll
                (
                    RequestedModelID,
                    RequestedRevisionIndex,
                    RequestedVersionIndex,
                    ModelObject.ModelOwnerUserID,
                    ModelObject.ModelSharedWithUserIDs,
                    AuthorizedUser.UserID,
                    JObject.Parse(JsonConvert.SerializeObject(VersionObject.FileEntry))
                ),
                _ErrorMessageAction);
            }

            var FileType = VersionObject.FileEntry.FileEntryFileType;
            VersionObject.FileEntry = new FileEntry()
            {
                FileEntryFileType = FileType
            };
            VersionObject.FileEntry.SetRelativeUrls_GetCommonUrlPart_FileEntryFileTypePreSet(RequestedModelID, RequestedRevisionIndex, RequestedVersionIndex);

            ModelObject.MRVLastUpdateTime = CommonMethods.GetTimeAsCreationTime();

            Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                new BPrimitiveType(RequestedModelID),
                JObject.Parse(JsonConvert.SerializeObject(ModelObject)));

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionVersionFileEntryDeleted
            (
                RequestedModelID,
                RequestedRevisionIndex,
                RequestedVersionIndex,
                ModelObject.ModelOwnerUserID,
                ModelObject.ModelSharedWithUserIDs,
                AuthorizedUser.UserID
            ),
            _ErrorMessageAction);

            return BWebResponse.StatusOK("Raw " 
                + (PreviousProcessStage == (int)Constants.EProcessStage.Uploaded_Processed ? "and processed models have " : "model has ")
                + "been deleted.");
        }

        private BWebServiceResponse GetRawFile(Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryGettingAllInfo(
                    DatabaseService,
                    RequestedModelID,
                    RequestedRevisionIndex,
                    RequestedVersionIndex,
                    out ModelDBEntry _,
                    out Revision _,
                    out RevisionVersion VersionObject,
                    out int _,
                    out BWebServiceResponse _FailureResponse,
                    _ErrorMessageAction))
            {
                return _FailureResponse;
            }

            if (VersionObject.FileEntry.FileProcessStage == (int)Constants.EProcessStage.NotUploaded)
            {
                return BWebResponse.NotFound("Raw file has not been uploaded yet.");
            }

            if (!FileService.CreateSignedURLForDownload(
                out string DownloadUrl, 
                CadFileStorageBucketName, 
                VersionObject.FileEntry.RawFileRelativeUrl,
                FileEntry.EXPIRY_MINUTES, 
                _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Signed url generation has failed.");
            }

            return BWebResponse.StatusOK("File has been located and download link has been generated.", new JObject()
            {
                [FileEntry.FILE_DOWNLOAD_URL_PROPERTY] = DownloadUrl,
                [FileEntry.FILE_DOWNLOAD_UPLOAD_EXPIRY_MINUTES_PROPERTY] = FileEntry.EXPIRY_MINUTES
            });
        }
    }
}