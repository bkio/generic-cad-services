/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
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
    internal class Model_RemoveModelShare : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        private string RequestedModelID;
        private string RequestedUserID;

        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly string RestfulUrlParameter_ModelsKey;
        private readonly string RestfulUrlParameter_UserIDKey;

        public Model_RemoveModelShare(
            IBDatabaseServiceInterface _DatabaseService, 
            string _RestfulUrlParameter_ModelsKey,
            string _RestfulUrlParameter_UserIDKey)
        {
            DatabaseService = _DatabaseService;
            RestfulUrlParameter_ModelsKey = _RestfulUrlParameter_ModelsKey;
            RestfulUrlParameter_UserIDKey = _RestfulUrlParameter_UserIDKey;
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

            if (_Context.Request.HttpMethod != "DELETE")
            {
                _ErrorMessageAction?.Invoke("Model_RemoveModelShare: DELETE method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("DELETE method is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Atomic operation control has failed.");
            }
            try
            {
                return ProcessRequestLocked(_Context, _ErrorMessageAction);
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction);
            }
        }

        private BWebServiceResponse ProcessRequestLocked(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
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
            RequestedUserID = RestfulUrlParameters[RestfulUrlParameter_UserIDKey];

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

            var OldSharedList = new List<string>(Model.ModelSharedWithUserIDs);
            if (!Model.ModelSharedWithUserIDs.Remove(RequestedUserID))
            {
                return BWebResponse.NotFound("Given userId is not among sharees of the model.");
            }

            Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                new BPrimitiveType(RequestedModelID),
                JObject.Parse(JsonConvert.SerializeObject(Model)));

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelSharedWithUserIdsChanged
            (
                RequestedModelID,
                Model.ModelOwnerUserID,
                Model.ModelSharedWithUserIDs,
                OldSharedList,
                AuthorizedUser.UserID
            ),
            _ErrorMessageAction);

            return BWebResponse.StatusOK("Operation has been completed.");
        }
    }
}