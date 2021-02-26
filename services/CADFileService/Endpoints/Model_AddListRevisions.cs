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
    internal class Model_AddListRevisions : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;

        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        private readonly string RestfulUrlParameter_ModelsKey;

        private string RequestedModelID;

        public Model_AddListRevisions(IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey)
        {
            DatabaseService = _DatabaseService;
            RestfulUrlParameter_ModelsKey = _RestfulUrlParameter_ModelsKey;
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

            if (_Context.Request.HttpMethod != "GET" && _Context.Request.HttpMethod != "PUT")
            {
                _ErrorMessageAction?.Invoke("Model_AddListRevisions: GET and PUT methods are accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("GET and PUT methods are accepted. But received request method: " + _Context.Request.HttpMethod);
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

            if (_Context.Request.HttpMethod == "GET")
            {
                return ListRevisions(_ErrorMessageAction);
            }
            else
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction))
                {
                    return BWebResponse.InternalError("Atomic operation control has failed.");
                }

                var Result = AddRevision(_Context, _ErrorMessageAction);

                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction);

                return Result;
            }
        }

        private BWebServiceResponse AddRevision(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryParsingRequestFor(
                _Context, out JObject _,
                true, out Revision NewRevisionObject,
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

            var UpperCaseNewRevisionName = NewRevisionObject.RevisionName.ToUpper();

            int BiggestExistingIndex = -1;
            foreach (var CurrentRev in Model.ModelRevisions)
            {
                if (CurrentRev.RevisionName.ToUpper() == UpperCaseNewRevisionName)
                {
                    return BWebResponse.Conflict("A revision with same " + Revision.REVISION_NAME_PROPERTY + " already exists.");
                }

                BiggestExistingIndex = BiggestExistingIndex < CurrentRev.RevisionIndex ? CurrentRev.RevisionIndex : BiggestExistingIndex;
            }
            int NewRevisionIndex = BiggestExistingIndex + 1;

            NewRevisionObject.RevisionIndex = NewRevisionIndex;
            NewRevisionObject.CreationTime = CommonMethods.GetTimeAsCreationTime();
            Model.MRVLastUpdateTime = NewRevisionObject.CreationTime;

            Model.ModelRevisions.Add(NewRevisionObject);

            Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                new BPrimitiveType(RequestedModelID),
                JObject.Parse(JsonConvert.SerializeObject(Model)));

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionCreated
            (
                RequestedModelID,
                NewRevisionObject.RevisionIndex,
                Model.ModelOwnerUserID,
                Model.ModelSharedWithUserIDs,
                AuthorizedUser.UserID
            ),
            _ErrorMessageAction);

            return BWebResponse.StatusCreated("Revision has been created.", new JObject()
            {
                [Revision.REVISION_INDEX_PROPERTY] = NewRevisionIndex
            });
        }

        private BWebServiceResponse ListRevisions(Action<string> _ErrorMessageAction)
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

            var Result = new JArray();

            foreach (var Rev in Model.ModelRevisions)
            {
                Rev.Prune_NonGettableProperties();
                Result.Add(JObject.Parse(JsonConvert.SerializeObject(Rev)));
            }

            return BWebResponse.StatusOK("List revisions operation has succeeded", new JObject()
            {
                [ModelDBEntry.MODEL_REVISIONS_PROPERTY] = Result
            });
        }
    }
}