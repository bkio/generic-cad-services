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
    internal class Model_GetUpdateDeleteRevision : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;

        private readonly string CadFileStorageBucketName;
        
        private readonly string RestfulUrlParameter_ModelsKey;
        private readonly string RestfulUrlParameter_RevisionsKey;

        private string RequestedModelID;
        private int RequestedRevisionIndex;

        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        public Model_GetUpdateDeleteRevision(IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey, string _RestfulUrlParameter_RevisionsKey, string _CadFileStorageBucketName)
        {
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

            if (_Context.Request.HttpMethod != "GET" && _Context.Request.HttpMethod != "POST" && _Context.Request.HttpMethod != "DELETE")
            {
                _ErrorMessageAction?.Invoke("Model_GetUpdateDeleteRevision: GET, POST and DELETE methods are accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("GET, POST and DELETE methods are accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            var RequestedModelName = RestfulUrlParameters[RestfulUrlParameter_ModelsKey];

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

            if (_Context.Request.HttpMethod == "GET")
            {
                return GetRevisionInfo(_ErrorMessageAction);
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
                    Result = DeleteRevision(_Context, _ErrorMessageAction);
                }
                else
                {
                    Result = UpdateRevisionInfo(_Context, _ErrorMessageAction);
                }

                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction);

                return Result;
            }
        }

        private BWebServiceResponse UpdateRevisionInfo(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryParsingRequestFor(
                _Context, 
                out JObject UpdatedRevisionJson,
                true, out Revision UpdatedRevision,
                out BWebServiceResponse FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
            }

            if (!CommonMethods.TryGettingModelInfo(
                DatabaseService,
                RequestedModelID,
                out JObject _,
                true, out ModelDBEntry Model,
                out FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
            }

            if (!CommonMethods.DoesRevisionExist(
                Model,
                RequestedRevisionIndex,
                out Revision RevisionObject,
                out int _))
            {
                return BWebResponse.NotFound("Revision does not exist.");
            }

            if (UpdatedRevision.RevisionName != null && UpdatedRevision.RevisionName.Length > 0 &&
                RevisionObject.RevisionName != UpdatedRevision.RevisionName)
            {
                //There is a change in the revision name
                foreach (var CurrentRev in Model.ModelRevisions)
                {
                    if (CurrentRev.RevisionName.ToUpper() == UpdatedRevision.RevisionName)
                    {
                        return BWebResponse.Conflict("A revision with same " + Revision.REVISION_NAME_PROPERTY + " already exists.");
                    }
                }
            }

            RevisionObject.Merge(UpdatedRevisionJson);
            Model.MRVLastUpdateTime = CommonMethods.GetTimeAsCreationTime();

            Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                new BPrimitiveType(RequestedModelID),
                JObject.Parse(JsonConvert.SerializeObject(Model)));

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionUpdated
            (
                RequestedModelID,
                RequestedRevisionIndex,
                Model.ModelOwnerUserID,
                Model.ModelSharedWithUserIDs,
                AuthorizedUser.UserID,
                UpdatedRevisionJson
            ),
            _ErrorMessageAction);

            return BWebResponse.StatusOK("Revision has been updated.");
        }

        private BWebServiceResponse DeleteRevision(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryGettingModelInfo(
                DatabaseService,
                RequestedModelID,
                out JObject _,
                true, out ModelDBEntry Model,
                out BWebServiceResponse FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
            }

            if (!CommonMethods.DoesRevisionExist(
                Model,
                RequestedRevisionIndex,
                out Revision RevisionObject,
                out int ModelRevisionListIx))
            {
                return BWebResponse.NotFound("Revision does not exist.");
            }

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionFileEntryDeleteAll
                (
                    RequestedModelID,
                    RequestedRevisionIndex,
                    Model.ModelOwnerUserID,
                    Model.ModelSharedWithUserIDs,
                    AuthorizedUser.UserID,
                    JObject.Parse(JsonConvert.SerializeObject(RevisionObject.FileEntry))
                ),
                _ErrorMessageAction);

            Model.ModelRevisions.RemoveAt(ModelRevisionListIx);
            Model.MRVLastUpdateTime = CommonMethods.GetTimeAsCreationTime();

            Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                new BPrimitiveType(RequestedModelID),
                JObject.Parse(JsonConvert.SerializeObject(Model)));

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionDeleted
            (
                RequestedModelID,
                RequestedRevisionIndex,
                Model.ModelOwnerUserID,
                Model.ModelSharedWithUserIDs,
                AuthorizedUser.UserID
            ),
            _ErrorMessageAction);

            return BWebResponse.StatusOK("Revision has been deleted.");
        }

        private BWebServiceResponse GetRevisionInfo(Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryGettingModelInfo(
                DatabaseService,
                RequestedModelID,
                out JObject _,
                true, out ModelDBEntry Model,
                out BWebServiceResponse FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
            }

            if (!CommonMethods.DoesRevisionExist(
                Model,
                RequestedRevisionIndex,
                out Revision RevisionObject,
                out int _))
            {
                return BWebResponse.NotFound("Revision does not exist.");
            }

            RevisionObject.Prune_NonGettableProperties();

            return BWebResponse.StatusOK("Get revision operation has succeeded.", JObject.Parse(JsonConvert.SerializeObject(RevisionObject)));
        }
    }
}