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
using ServiceUtilities.PubSubUsers.PubSubRelated;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CADFileService
{
    internal class Model_GetUpdateDeleteRevisionVersion : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly string CadFileStorageBucketName;

        private readonly string RestfulUrlParameter_ModelsKey;
        private readonly string RestfulUrlParameter_RevisionsKey;
        private readonly string RestfulUrlParameter_VersionsKey;

        private string RequestedModelID;
        private int RequestedRevisionIndex;
        private int RequestedVersionIndex;

        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        public Model_GetUpdateDeleteRevisionVersion(IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey, string _RestfulUrlParameter_RevisionsKey, string _RestfulUrlParameter_VersionsKey, string _CadFileStorageBucketName)
        {
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
            
            if (_Context.Request.HttpMethod != "GET" && _Context.Request.HttpMethod != "POST" && _Context.Request.HttpMethod != "DELETE")
            {
                _ErrorMessageAction?.Invoke("Model_GetUpdateDeleteRevisionVersion: GET, POST and DELETE methods are accepted. But received request method:  " + _Context.Request.HttpMethod);
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

            if (_Context.Request.HttpMethod == "GET")
            {
                return GetRevisionVersionInfo(_ErrorMessageAction);
            }
            else
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction))
                {
                    return BWebResponse.InternalError("Atomic operation control has failed.");
                }

                BWebServiceResponse Result;
                if (_Context.Request.HttpMethod == "DELETE")
                {
                    Result = DeleteRevisionVersion(_Context, _ErrorMessageAction);
                }
                else
                {
                    Result = UpdateRevisionVersionInfo(_Context, _ErrorMessageAction);
                }

                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction);

                return Result;
            }
        }

        private BWebServiceResponse UpdateRevisionVersionInfo(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryParsingRequestFor(
                _Context,
                out JObject UpdatedVersionJson,
                true, out RevisionVersion UpdatedVersion,
                out BWebServiceResponse FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
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

            if (UpdatedVersion.VersionName != null && UpdatedVersion.VersionName.Length > 0 &&
                VersionObject.VersionName != UpdatedVersion.VersionName)
            {
                //There is a change in the version name
                foreach (var CurrentVer in RevisionObject.RevisionVersions)
                {
                    if (CurrentVer.VersionName.ToUpper() == UpdatedVersion.VersionName)
                    {
                        return BWebResponse.Conflict("A revision with same " + RevisionVersion.VERSION_NAME_PROPERTY + " already exists.");
                    }
                }
            }

            VersionObject.Merge(UpdatedVersionJson);
            ModelObject.MRVLastUpdateTime = CommonMethods.GetTimeAsCreationTime();

            Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                new BPrimitiveType(RequestedModelID),
                JObject.Parse(JsonConvert.SerializeObject(ModelObject)));

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionVersionUpdated
            (
                RequestedModelID,
                RequestedRevisionIndex,
                RequestedVersionIndex,
                ModelObject.ModelOwnerUserID,
                ModelObject.ModelSharedWithUserIDs,
                AuthorizedUser.UserID,
                UpdatedVersionJson
            ),
            _ErrorMessageAction);

            return BWebResponse.StatusOK("Revision version has been updated.");
        }

        private BWebServiceResponse DeleteRevisionVersion(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryGettingAllInfo(
                DatabaseService,
                RequestedModelID,
                RequestedRevisionIndex,
                RequestedVersionIndex,
                out ModelDBEntry ModelObject,
                out Revision RevisionObject,
                out RevisionVersion VersionObject,
                out int ModelRevisionVersionListIx,
                out BWebServiceResponse _FailureResponse,
                _ErrorMessageAction))
            {
                return _FailureResponse;
            }

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

            RevisionObject.RevisionVersions.RemoveAt(ModelRevisionVersionListIx);
            ModelObject.MRVLastUpdateTime = CommonMethods.GetTimeAsCreationTime();

            Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                new BPrimitiveType(RequestedModelID),
                JObject.Parse(JsonConvert.SerializeObject(ModelObject)));

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionVersionDeleted
            (
                RequestedModelID,
                RequestedRevisionIndex,
                RequestedVersionIndex,
                ModelObject.ModelOwnerUserID,
                ModelObject.ModelSharedWithUserIDs,
                AuthorizedUser.UserID
            ),
            _ErrorMessageAction);

            return BWebResponse.StatusOK("Revision version has been deleted.");
        }

        private BWebServiceResponse GetRevisionVersionInfo(Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryGettingAllInfo(
                DatabaseService,
                RequestedModelID,
                RequestedRevisionIndex,
                RequestedVersionIndex,
                out ModelDBEntry ModelObject,
                out Revision RevisionObject,
                out RevisionVersion VersionObject,
                out int _,
                out BWebServiceResponse _FailureResponse,
                _ErrorMessageAction))
            {
                return _FailureResponse;
            }

            VersionObject.Prune_NonGettableProperties();

            return BWebResponse.StatusOK("Get revision version operation has succeeded.", JObject.Parse(JsonConvert.SerializeObject(VersionObject)));
        }
    }
}