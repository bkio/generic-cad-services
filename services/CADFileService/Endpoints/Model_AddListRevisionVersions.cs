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
    internal class Model_AddListRevisionVersions : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;

        private readonly string RestfulUrlParameter_ModelsKey;
        private readonly string RestfulUrlParameter_RevisionsKey;

        private string RequestedModelID;
        private int RequestedRevisionIndex;

        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        public Model_AddListRevisionVersions(IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey, string _RestfulUrlParameter_RevisionsKey)
        {
            DatabaseService = _DatabaseService;
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

            //For instance if metadata contains projectName; projectName shall be key and index of this model shall be in the value array
            //For model domain. Not for revision metadata.

            if (_Context.Request.HttpMethod != "GET" && _Context.Request.HttpMethod != "PUT")
            {
                _ErrorMessageAction?.Invoke("Model_AddListRevisionVersions: GET and PUT methods are accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("GET and PUT methods are accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            RequestedModelID = RestfulUrlParameters[RestfulUrlParameter_ModelsKey];
            if (!int.TryParse(RestfulUrlParameters[RestfulUrlParameter_RevisionsKey], out RequestedRevisionIndex))
            {
                return BWebResponse.BadRequest("Revision index must be an integer.");
            }

            if (_Context.Request.HttpMethod == "GET")
            {
                return ListRevisionVersions(_ErrorMessageAction);
            }
            else
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction))
                {
                    return BWebResponse.InternalError("Atomic operation control has failed.");
                }

                var Result = AddRevisionVersion(_Context, _ErrorMessageAction);

                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction);

                return Result;
            }
        }

        private BWebServiceResponse AddRevisionVersion(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryParsingRequestFor(
                _Context, out JObject _,
                true, out RevisionVersion NewVersionObject,
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

            var UpperCaseNewVersionName = NewVersionObject.VersionName.ToUpper();

            int BiggestExistingIndex = -1;
            foreach (var CurrentVer in RevisionObject.RevisionVersions)
            {
                if (CurrentVer.VersionName.ToUpper() == UpperCaseNewVersionName)
                {
                    return BWebResponse.Conflict("A version with same " + RevisionVersion.VERSION_NAME_PROPERTY + " already exists.");
                }

                BiggestExistingIndex = BiggestExistingIndex < CurrentVer.VersionIndex ? CurrentVer.VersionIndex : BiggestExistingIndex;
            }
            int NewVersionIndex = BiggestExistingIndex + 1;

            NewVersionObject.VersionIndex = NewVersionIndex;
            NewVersionObject.CreationTime = CommonMethods.GetTimeAsCreationTime();
            Model.MRVLastUpdateTime = NewVersionObject.CreationTime;

            //NewVersionObject.FileEntry.SetRelativeUrls_GetCommonUrlPart_FileEntryFileTypePreSet(RequestedModelID, RequestedRevisionIndex, NewVersionIndex);

            RevisionObject.RevisionVersions.Add(NewVersionObject);

            Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                new BPrimitiveType(RequestedModelID),
                JObject.Parse(JsonConvert.SerializeObject(Model)));

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionVersionCreated
            (
                RequestedModelID,
                RequestedRevisionIndex,
                NewVersionIndex,
                Model.ModelOwnerUserID,
                Model.ModelSharedWithUserIDs,
                AuthorizedUser.UserID
            ),
            _ErrorMessageAction);

            return BWebResponse.StatusCreated("Revision version has been created.", new JObject()
            {
                [RevisionVersion.VERSION_INDEX_PROPERTY] = NewVersionIndex
            });
        }

        private BWebServiceResponse ListRevisionVersions(Action<string> _ErrorMessageAction)
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

            if (!CommonMethods.DoesRevisionExist(
                Model,
                RequestedRevisionIndex,
                out Revision RevisionObject,
                out int _))
            {
                return BWebResponse.NotFound("Revision does not exist.");
            }

            foreach (var Ver in RevisionObject.RevisionVersions)
            {
                Ver.Prune_NonGettableProperties();
                Result.Add(JObject.Parse(JsonConvert.SerializeObject(Ver)));
            }

            return BWebResponse.StatusOK("List revision versions operation has succeeded.", new JObject()
            {
                [Revision.REVISION_VERSIONS_PROPERTY] = Result
            });
        }
    }
}