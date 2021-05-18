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
using ServiceUtilities.Common;

namespace CADFileService
{
    internal class Model_AddListModels : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;

        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        public Model_AddListModels(IBDatabaseServiceInterface _DatabaseService)
        {
            DatabaseService = _DatabaseService;
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
                _ErrorMessageAction?.Invoke("Model_AddListModels: GET and PUT methods are accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("GET and PUT methods are accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            if (_Context.Request.HttpMethod == "GET")
            {
                return ListModels(_ErrorMessageAction);
            }
            return CreateModel(_Context, _ErrorMessageAction); //Atomicness handled inside
        }

        private BWebServiceResponse CreateModel(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryParsingRequestFor(
                _Context, out JObject _,
                true, out ModelDBEntry NewModelObject,
                out BWebServiceResponse FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
            }
            NewModelObject.ModelOwnerUserID = AuthorizedUser.UserID;

            if (!Methods.GenerateNonExistentUniqueID(
                this, 
                DatabaseService, ModelDBEntry.DBSERVICE_MODELS_TABLE(), ModelDBEntry.KEY_NAME_MODEL_ID, ModelDBEntry.MustHaveProperties,
                EGetClearance.Yes,
                out string ModelID,
                out BWebServiceResponse _FailureResponse,
                _ErrorMessageAction))
            {
                return _FailureResponse;
            }

            bool bGetClearanceForModelName = false;
            try
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(), UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME + ":" + NewModelObject.ModelName, _ErrorMessageAction))
                {
                    return BWebResponse.InternalError("Atomic operation control has failed.");
                }
                bGetClearanceForModelName = true;

                if (!DatabaseService.UpdateItem(
                    UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(),
                    UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME,
                    new BPrimitiveType(NewModelObject.ModelName),
                    new JObject()
                    {
                        [ModelDBEntry.KEY_NAME_MODEL_ID] = ModelID
                    },
                    out JObject _,
                    EBReturnItemBehaviour.DoNotReturn,
                    DatabaseService.BuildAttributeNotExistCondition(UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME),
                    _ErrorMessageAction))
                {
                    if (!DatabaseService.GetItem(
                        UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(),
                        UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME,
                        new BPrimitiveType(NewModelObject.ModelName),
                        UniqueFileFieldsDBEntry.Properties,
                        out JObject ModelIDResponse,
                        _ErrorMessageAction) || !ModelIDResponse.ContainsKey(ModelDBEntry.KEY_NAME_MODEL_ID))
                    {
                        return BWebResponse.InternalError("Model ID could not be retrieved upon conflict.");
                    }
                    var Result = JObject.Parse(BWebResponse.Error_Conflict_String("Attribute " + UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME + " must be globally unique."));
                    Result[ModelDBEntry.KEY_NAME_MODEL_ID] = (string)ModelIDResponse[ModelDBEntry.KEY_NAME_MODEL_ID];
                    return new BWebServiceResponse(
                        BWebResponse.Error_Conflict_Code, 
                        new BStringOrStream(Result.ToString()),
                        BWebResponse.Error_Conflict_ContentType);
                }

                if (!Controller_AttributeTables.Get().AddRemoveMetadataSets_AttributesTables(
                    InnerDeliveryEnsurerUserProcessor, NewModelObject.ModelOwnerUserID,
                    Controller_AttributeTables.MetadataLocator.ItIsModelMetadata(ModelID),
                    NewModelObject.ModelMetadata,
                    Controller_AttributeTables.EAddRemove.Add,
                    Controller_AttributeTables.EKillProcedureIfGetClearanceFails.Yes,
                    out _FailureResponse,
                    _ErrorMessageAction))
                {
                    return _FailureResponse;
                }

                NewModelObject.CreationTime = CommonMethods.GetTimeAsCreationTime();
                NewModelObject.MRVLastUpdateTime = NewModelObject.CreationTime;

                Controller_DeliveryEnsurer.Get().DB_PutItem_FireAndForget(
                    _Context,
                    ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                    ModelDBEntry.KEY_NAME_MODEL_ID,
                    new BPrimitiveType(ModelID),
                    JObject.Parse(JsonConvert.SerializeObject(NewModelObject)));

                Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelCreated
                (
                    ModelID,
                    AuthorizedUser.UserID,
                    new List<string>(),
                    AuthorizedUser.AuthMethodKey
                ),
                _ErrorMessageAction);

                return BWebResponse.StatusCreated("Model has been created.", new JObject()
                {
                    [ModelDBEntry.KEY_NAME_MODEL_ID] = ModelID
                });
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), ModelID, _ErrorMessageAction);
                if (bGetClearanceForModelName)
                {
                    Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(), UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME + ":" + NewModelObject.ModelName, _ErrorMessageAction);
                }
            }
        }

        private BWebServiceResponse ListModels(Action<string> _ErrorMessageAction)
        {
            if (!DatabaseService.ScanTable(ModelDBEntry.DBSERVICE_MODELS_TABLE(), out List<JObject> ModelsJson, _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Scan-table operation has failed.");
            }

            var Result = new JObject();
            var ModelsArray = new JArray();
            Result["models"] = ModelsArray;

            foreach (var ModelJson in ModelsJson)
            {
                var AsModel = JsonConvert.DeserializeObject<ModelDBEntry>(ModelJson.ToString());
                AsModel.Prune_NonGettableProperties();
                ModelsArray.Add(JObject.Parse(ModelJson.ToString()));
            }

            return BWebResponse.StatusOK("List models operation has succeeded.", Result);
        }
    }
}