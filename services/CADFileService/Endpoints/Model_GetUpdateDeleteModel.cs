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
    internal class Model_GetUpdateDeleteModel : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;

        //NOTE: authorized-u-auth-key could be "INTERNAL_CALL" Check CleanupCall.cs in AuthService
        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        private readonly string RestfulUrlParameter_ModelsKey;

        private string RequestedModelID;

        public Model_GetUpdateDeleteModel(IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey)
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

            if (_Context.Request.HttpMethod != "GET" && _Context.Request.HttpMethod != "POST" && _Context.Request.HttpMethod != "DELETE")
            {
                _ErrorMessageAction?.Invoke("Model_GetUpdateDeleteModel: GET, POST and DELETE methods are accepted. But received request method:  " + _Context.Request.HttpMethod);
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

            if (_Context.Request.HttpMethod == "GET")
            {
                return GetModelInfo(_ErrorMessageAction);
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
                    Result = DeleteModel(_Context, _ErrorMessageAction);
                }
                else
                {
                    Result = UpdateModelInfo(_Context, _ErrorMessageAction);
                }

                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction);

                return Result;
            }
        }

        private BWebServiceResponse UpdateModelInfo(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (!CommonMethods.TryParsingRequestFor(
                _Context, out JObject UpdateFieldsModelEntry,
                false, out ModelDBEntry _,
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

            //If there is a change in the model name
            bool bGetClearanceForModelName_New = false;
            bool bGetClearanceForModelName_Old = false;
            string ModelUniqueName_New = null;
            string ModelUniqueName_Old = null;

            try
            {
                if (UpdateFieldsModelEntry.ContainsKey(ModelDBEntry.MODEL_UNIQUE_NAME_PROPERTY)
                    && Model.ModelName != (string)UpdateFieldsModelEntry[ModelDBEntry.MODEL_UNIQUE_NAME_PROPERTY])
                {
                    ModelUniqueName_New = (string)UpdateFieldsModelEntry[ModelDBEntry.MODEL_UNIQUE_NAME_PROPERTY];
                    if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(), UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME + ":" + ModelUniqueName_New, _ErrorMessageAction))
                    {
                        return BWebResponse.InternalError("Atomic operation control has failed.");
                    }
                    bGetClearanceForModelName_New = true;

                    ModelUniqueName_Old = Model.ModelName;
                    if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(), UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME + ":" + ModelUniqueName_Old, _ErrorMessageAction))
                    {
                        return BWebResponse.InternalError("Atomic operation control has failed.");
                    }
                    bGetClearanceForModelName_Old = true;

                    //Put the new one
                    if (!DatabaseService.UpdateItem(
                        UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(),
                        UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME,
                        new BPrimitiveType(ModelUniqueName_New),
                        new JObject()
                        {
                            [ModelDBEntry.KEY_NAME_MODEL_ID] = RequestedModelID
                        },
                        out JObject _,
                        EBReturnItemBehaviour.DoNotReturn,
                        DatabaseService.BuildAttributeNotExistCondition(UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME),
                        _ErrorMessageAction))
                    {
                        if (!DatabaseService.GetItem(
                            UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(),
                            UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME,
                            new BPrimitiveType(ModelUniqueName_New),
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

                    //Delete the old one
                    Controller_DeliveryEnsurer.Get().DB_DeleteItem_FireAndForget(
                        _Context,
                        UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(),
                        UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME,
                        new BPrimitiveType(ModelUniqueName_Old));
                }

                var OldModelMetadataSet = Model.ModelMetadata;

                Model.Merge(UpdateFieldsModelEntry);

                var NewModelMetadataSet = Model.ModelMetadata;

                if (OldModelMetadataSet != NewModelMetadataSet)
                {
                    var MetaLocator = Controller_AttributeTables.MetadataLocator.ItIsModelMetadata(RequestedModelID);

                    //First remove all metadata sets
                    Controller_AttributeTables.Get().AddRemoveMetadataSets_AttributesTables(
                        InnerDeliveryEnsurerUserProcessor, Model.ModelOwnerUserID,
                        MetaLocator,
                        OldModelMetadataSet,
                        Controller_AttributeTables.EAddRemove.Remove,
                        Controller_AttributeTables.EKillProcedureIfGetClearanceFails.No,
                        out BWebServiceResponse _, _ErrorMessageAction);

                    //Then add new metadata sets
                    Controller_AttributeTables.Get().AddRemoveMetadataSets_AttributesTables(
                        InnerDeliveryEnsurerUserProcessor, Model.ModelOwnerUserID,
                        MetaLocator,
                        NewModelMetadataSet,
                        Controller_AttributeTables.EAddRemove.Add,
                        Controller_AttributeTables.EKillProcedureIfGetClearanceFails.No,
                        out BWebServiceResponse _, _ErrorMessageAction);
                }
                
                Model.MRVLastUpdateTime = CommonMethods.GetTimeAsCreationTime();

                Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                    _Context,
                    ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                    ModelDBEntry.KEY_NAME_MODEL_ID,
                    new BPrimitiveType(RequestedModelID),
                    JObject.Parse(JsonConvert.SerializeObject(Model)));

                Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelUpdated
                (
                    RequestedModelID,
                    Model.ModelOwnerUserID,
                    Model.ModelSharedWithUserIDs,
                    AuthorizedUser.UserID,
                    UpdateFieldsModelEntry
                ),
                _ErrorMessageAction);

                return BWebResponse.StatusOK("Model has been successfully updated.");
            }
            finally
            {
                if (bGetClearanceForModelName_New)
                {
                    Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(), UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME + ":" + ModelUniqueName_New, _ErrorMessageAction);
                }
                if (bGetClearanceForModelName_Old)
                {
                    Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(), UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME + ":" + ModelUniqueName_Old, _ErrorMessageAction);
                }
            }
        }

        private BWebServiceResponse GetModelInfo(Action<string> _ErrorMessageAction)
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

            Model.Prune_NonGettableProperties();

            return BWebResponse.StatusOK("Get model operation has succeeded.", JObject.Parse(JsonConvert.SerializeObject(Model)));
        }

        private BWebServiceResponse DeleteModel(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
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
            
            if (!Controller_AttributeTables.Get().AddRemoveMetadataSets_AttributesTables(
                InnerDeliveryEnsurerUserProcessor, Model.ModelOwnerUserID,
                Controller_AttributeTables.MetadataLocator.ItIsModelMetadata(RequestedModelID),
                Model.ModelMetadata,
                Controller_AttributeTables.EAddRemove.Remove,
                Controller_AttributeTables.EKillProcedureIfGetClearanceFails.Yes,
                out FailureResponse, _ErrorMessageAction))
            {
                return FailureResponse;
            }

            foreach (var Rev in Model.ModelRevisions)
            {
                Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionFileEntryDeleteAll
                (
                    RequestedModelID,
                    Rev.RevisionIndex,
                    Model.ModelOwnerUserID,
                    Model.ModelSharedWithUserIDs,
                    AuthorizedUser.UserID,
                    JObject.Parse(JsonConvert.SerializeObject(Rev.FileEntry))
                ),
                _ErrorMessageAction);
            }

            Controller_DeliveryEnsurer.Get().DB_DeleteItem_FireAndForget(
                _Context,
                UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(),
                UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME,
                new BPrimitiveType(Model.ModelName));

            var RequestedModelIDPrimitive = new BPrimitiveType(RequestedModelID);

            Controller_DeliveryEnsurer.Get().DB_DeleteItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                RequestedModelIDPrimitive);

            if (Model.ModelSharedWithUserIDs.Contains("*"))
            {
                Controller_DeliveryEnsurer.Get().DB_DeleteItem_FireAndForget(
                    _Context,
                    GloballySharedModelIDsDBEntry.DBSERVICE_GLOBALLY_SHARED_MODEL_IDS_TABLE(),
                    GloballySharedModelIDsDBEntry.KEY_NAME_MODEL_ID,
                    RequestedModelIDPrimitive);
            }
            
            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelDeleted
            (
                RequestedModelID,
                Model.ModelOwnerUserID,
                Model.ModelSharedWithUserIDs,
                AuthorizedUser.UserID
            ),
            _ErrorMessageAction);

            return BWebResponse.StatusOK("Model has been deleted.");
        }
    }
}