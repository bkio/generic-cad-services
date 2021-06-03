/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADFileService.Controllers;
using CADFileService.Endpoints.Common;
using CADFileService.Endpoints.Structures;
using ServiceUtilities;
using ServiceUtilities.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CADFileService.Endpoints
{
    partial class InternalCalls
    {
        internal class PubSub_To_CadFileService : PubSubServiceBaseWebhookTimeoutableDeliveryEnsurerUser
        {
            private readonly IBDatabaseServiceInterface DatabaseService;
            private readonly IBFileServiceInterface FileService;

            private readonly string AzureStorageServiceUrl;
            private readonly string CadFileStorageBucketName;
            private readonly string CadProcessServiceEndpoint;

            public PubSub_To_CadFileService(
                string _InternalCallPrivateKey, 
                IBDatabaseServiceInterface _DatabaseService, 
                IBFileServiceInterface _FileService, 
                string _AzureStorageServiceUrl, 
                string _CadFileStorageBucketName, 
                string _CadProcessServiceEndpoint) : base(_InternalCallPrivateKey)
            {
                DatabaseService = _DatabaseService;
                FileService = _FileService;
                AzureStorageServiceUrl = _AzureStorageServiceUrl;
                CadFileStorageBucketName = _CadFileStorageBucketName;
                CadProcessServiceEndpoint = _CadProcessServiceEndpoint;
            }

            protected override bool Handle(HttpListenerContext _Context, ServiceUtilities.Action _Action, Action<string> _ErrorMessageAction = null)
            {
                _ErrorMessageAction?.Invoke($"Info: PubSub_To_CadFileService->Handle: {_Action.GetActionType().ToString()}");
                if (_Action.GetActionType() == Actions.EAction.ACTION_CAD_FILE_SERVICE_DELIVERY_ENSURER)
                {
                    Controller_DeliveryEnsurer.Get().Retry_FireAndForget_Operation(_Context, (Action_DeliveryEnsurer)_Action, _ErrorMessageAction);
                }
                else if (_Action.GetActionType() == Actions.EAction.ACTION_USER_DELETED)
                {
                    return DeleteUserModelsUpdateSharings(_Context, (Action_UserDeleted)_Action, _ErrorMessageAction);
                }
                else if (_Action.GetActionType() == Actions.EAction.ACTION_MODEL_REVISION_FILE_ENTRY_DELETE_ALL)
                {
                    return DeleteFiles(_Context, (Action_ModelRevisionFileEntryDeleteAll)_Action, _ErrorMessageAction);
                }
                else if (_Action.GetActionType() == Actions.EAction.ACTION_STORAGE_FILE_UPLOADED_CLOUDEVENT)
                {
                    var Casted = (Action_StorageFileUploaded_CloudEventSchemaV1_0)_Action;
                    var _ServiceEndpointPart = AzureStorageServiceUrl + CadFileStorageBucketName + "/";
                    Casted.ConvertUrlToRelativeUrl(_ServiceEndpointPart);

                    return FileUploaded(_Context, Casted, _ErrorMessageAction);
                }
                else if (_Action.GetActionType() == Actions.EAction.ACTION_BATCH_PROCESS_FAILED)
                {
                    var Casted = (Action_BatchProcessFailed)_Action;

                    return UpdateModelBatchProcessFailed(_Context, Casted, _ErrorMessageAction);
                }
                //else: Other cad file service related actions

                return true;
            }

            private bool UpdateModelBatchProcessFailed(HttpListenerContext _Context, Action_BatchProcessFailed _Action, Action<string> _ErrorMessageAction = null)
            {
                if (!CommonMethods.TryGettingModelID(
                    DatabaseService,
                    _Action.ModelName,
                    out string _ModelID,
                    out BWebServiceResponse FailureResponse,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke($"Error: PubSub_Handler->UpdateModelBatchProcessFailed: ModelID could not find with this ModelName: {_Action.ModelName}->{_Action.RevisionIndex}");
                    return false;
                }

                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), _ModelID, _ErrorMessageAction))
                {
                    return false; //Retry
                }
                try
                {
                    if (!CommonMethods.TryGettingAllInfo(
                        DatabaseService,
                        _ModelID,
                        _Action.RevisionIndex,
                        out ModelDBEntry ModelObject,
                        out Revision RevisionObject,
                        out int _,
                        out BWebServiceResponse _FailureResponse,
                        _ErrorMessageAction))
                    {
                        if (_FailureResponse.StatusCode == BWebResponse.Error_NotFound_Code)
                        {
                            _ErrorMessageAction?.Invoke($"Error: PubSub_Handler->UpdateModelBatchProcessFailed: Model/revision does not exist: {_Action.ModelName}->{_Action.RevisionIndex}");
                            return true; //Should return 200
                        }
                        return false; //DB Error - Retry
                    }

                    RevisionObject.FileEntry.FileUploadProcessStage = (int)EUploadProcessStage.Uploaded_ProcessFailed;
                    ModelObject.MRVLastUpdateTime = CommonMethods.GetTimeAsCreationTime();

                    Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                        _Context,
                        ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                        ModelDBEntry.KEY_NAME_MODEL_ID,
                        new BPrimitiveType(_ModelID),
                        JObject.Parse(JsonConvert.SerializeObject(ModelObject)));

                    return true;
                }
                finally
                {
                    Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), _ModelID, _ErrorMessageAction);
                }
            }

            private bool DeleteUserModelsUpdateSharings(HttpListenerContext _Context, Action_UserDeleted _Action, Action<string> _ErrorMessageAction = null)
            {
                try
                {
                    var SetClearanceForModels = new List<string>(_Action.UserModels);
                    SetClearanceForModels.AddRange(_Action.UserSharedModels);

                    if (GetClearanceForAllModels(SetClearanceForModels, _ErrorMessageAction))
                    {
                        var ParallelOperationsStack = new Stack<bool>();
                        for (var i = 0; i < _Action.UserModels.Count; i++) ParallelOperationsStack.Push(true);
                        for (var i = 0; i < _Action.UserSharedModels.Count; i++) ParallelOperationsStack.Push(true);
                        var bWait = ParallelOperationsStack.Count > 0;

                        var WaitFor = new ManualResetEvent(false);

                        var InternalErrorOccured = new BValue<bool>(false, EBProducerStatus.MultipleProducer);
                        foreach (var ModelID in _Action.UserModels)
                        {
                            BTaskWrapper.Run(() =>
                            {
                                DeleteUserModel(_Context, InternalErrorOccured, ModelID, _Action.UserID, _ErrorMessageAction);

                                if (InternalErrorOccured.Get())
                                {
                                    try
                                    {
                                        WaitFor.Set();
                                    }
                                    catch (Exception) { }
                                    return;
                                }

                                lock (ParallelOperationsStack)
                                {
                                    ParallelOperationsStack.TryPop(out bool _);
                                    if (ParallelOperationsStack.Count == 0)
                                    {
                                        try
                                        {
                                            WaitFor.Set();
                                        }
                                        catch (Exception) { }
                                    }
                                }
                            });
                        }
                        foreach (var SharedModelID in _Action.UserSharedModels)
                        {
                            BTaskWrapper.Run(() =>
                            {
                                DeleteUserSharings(_Context, InternalErrorOccured, SharedModelID, _Action.UserID, _ErrorMessageAction);

                                if (InternalErrorOccured.Get())
                                {
                                    try
                                    {
                                        WaitFor.Set();
                                    }
                                    catch (Exception) { }
                                    return;
                                }

                                lock (ParallelOperationsStack)
                                {
                                    ParallelOperationsStack.TryPop(out bool _);
                                    if (ParallelOperationsStack.Count == 0)
                                    {
                                        try
                                        {
                                            WaitFor.Set();
                                        }
                                        catch (Exception) { }
                                    }
                                }
                            });
                        }

                        try
                        {
                            if (bWait)
                            {
                                WaitFor.WaitOne();
                            }
                            WaitFor.Close();
                        }
                        catch (Exception) { }

                        return !InternalErrorOccured.Get();
                    }
                }
                finally
                {
                    SetClearanceForObtainedModels();
                }
                return false;
            }

            private void DeleteUserModel(HttpListenerContext _Context, BValue<bool> _InternalErrorOccured, string _ModelID, string _DeletedUserID, Action<string> _ErrorMessageAction)
            {
                var ModelKey = new BPrimitiveType(_ModelID);

                if (!CommonMethods.TryGettingModelInfo(
                    DatabaseService,
                    _ModelID,
                    out JObject _,
                    true, out ModelDBEntry ModelData,
                    out BWebServiceResponse _FailedResponse,
                    _ErrorMessageAction))
                {
                    if (_FailedResponse.StatusCode == BWebResponse.Error_NotFound_Code)
                    {
                        _ErrorMessageAction?.Invoke("Warning: PubSub_Handler->DeleteUserModel: Model does not exist: " + _ModelID);
                    }
                    else
                    {
                        _InternalErrorOccured.Set(true); //Internal error, return error for retrial.
                    }
                    return;
                }

                if (ModelData.ModelSharedWithUserIDs.Contains("*"))
                {
                    Controller_DeliveryEnsurer.Get().DB_DeleteItem_FireAndForget(
                        _Context,
                        GloballySharedModelIDsDBEntry.DBSERVICE_GLOBALLY_SHARED_MODEL_IDS_TABLE(),
                        GloballySharedModelIDsDBEntry.KEY_NAME_MODEL_ID,
                        new BPrimitiveType(_ModelID));
                }

                foreach (var Rev in ModelData.ModelRevisions)
                {
                    Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionFileEntryDeleteAll
                        (
                            _ModelID,
                            Rev.RevisionIndex,
                            ModelData.ModelOwnerUserID,
                            ModelData.ModelSharedWithUserIDs,
                            _DeletedUserID,
                            JObject.Parse(JsonConvert.SerializeObject(Rev.FileEntry))
                        ),
                        _ErrorMessageAction);
                }

                Controller_DeliveryEnsurer.Get().DB_DeleteItem_FireAndForget(
                    _Context,
                    ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                    ModelDBEntry.KEY_NAME_MODEL_ID,
                    ModelKey);

                Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelDeleted
                (
                    _ModelID,
                    ModelData.ModelOwnerUserID,
                    ModelData.ModelSharedWithUserIDs,
                    _DeletedUserID
                ),
                _ErrorMessageAction);
            }

            private void DeleteUserSharings(HttpListenerContext _Context, BValue<bool> _InternalErrorOccured, string _SharedModelID, string _DeletedUserID, Action<string> _ErrorMessageAction)
            {
                var ModelKey = new BPrimitiveType(_SharedModelID);

                if (!CommonMethods.TryGettingModelInfo(
                    DatabaseService,
                    _SharedModelID,
                    out JObject _,
                    true, out ModelDBEntry ModelData,
                    out BWebServiceResponse _FailedResponse,
                    _ErrorMessageAction))
                {
                    if (_FailedResponse.StatusCode == BWebResponse.Error_NotFound_Code)
                    {
                        _ErrorMessageAction?.Invoke("Warning: PubSub_Handler->DeleteUserSharings: Model does not exist: " + ModelKey);
                    }
                    else
                    {
                        _InternalErrorOccured.Set(true); //Internal error, return error for retrial.
                    }
                    return;
                }

                if (!ModelData.ModelSharedWithUserIDs.Contains("*"))
                {
                    if (!ModelData.ModelSharedWithUserIDs.Remove(_DeletedUserID))
                    {
                        _ErrorMessageAction?.Invoke("Warning: PubSub_Handler->DeleteUserSharings: User does not exist in shared list of the model: " + _SharedModelID);
                        return;
                    }

                    Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                        _Context,
                        ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                        ModelDBEntry.KEY_NAME_MODEL_ID,
                        ModelKey,
                        JObject.Parse(JsonConvert.SerializeObject(ModelData)));
                }
            }

            private readonly Stack<string> UserModelsClearanceObtained = new Stack<string>();
            private bool GetClearanceForAllModels(List<string> _UserModels, Action<string> _ErrorMessageAction = null)
            {
                var ParallelOperationsStack = new Stack<bool>();
                for (var i = 0; i < _UserModels.Count; i++) ParallelOperationsStack.Push(true);

                int FailedClearanceOps = 0;

                var WaitFor = new ManualResetEvent(false);

                foreach (var ModelID in _UserModels)
                {
                    BTaskWrapper.Run(() =>
                    {
                        if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(
                            InnerProcessor,
                            ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                            ModelID,
                            _ErrorMessageAction))
                        {
                            Interlocked.Increment(ref FailedClearanceOps);
                            return;
                        }
                        lock (UserModelsClearanceObtained)
                        {
                            UserModelsClearanceObtained.Push(ModelID);
                        }
                        lock (ParallelOperationsStack)
                        {
                            ParallelOperationsStack.TryPop(out bool _);
                            if (ParallelOperationsStack.Count == 0)
                            {
                                try
                                {
                                    WaitFor.Set();
                                }
                                catch (Exception) { }
                            }
                        }
                    });
                }

                try
                {
                    if (_UserModels.Count > 0)
                    {
                        WaitFor.WaitOne();
                    }
                    WaitFor.Close();
                }
                catch (Exception) { }

                return FailedClearanceOps == 0;
            }
            private void SetClearanceForObtainedModels(Action<string> _ErrorMessageAction = null)
            {
                var WaitFor = new ManualResetEvent(false);
                bool bWait = false;

                lock (UserModelsClearanceObtained)
                {
                    var ParallelOperationsStack = new Stack<bool>();
                    if (UserModelsClearanceObtained.Count > 0)
                    {
                        bWait = true;
                        for (var i = 0; i < UserModelsClearanceObtained.Count; i++) ParallelOperationsStack.Push(true);
                    }

                    while (UserModelsClearanceObtained.TryPop(out string ClearanceObtainedModelID))
                    {
                        BTaskWrapper.Run(() =>
                        {
                            Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(
                                InnerProcessor,
                                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                                ClearanceObtainedModelID,
                                _ErrorMessageAction);

                            lock (ParallelOperationsStack)
                            {
                                ParallelOperationsStack.TryPop(out bool _);
                                if (ParallelOperationsStack.Count == 0)
                                {
                                    try
                                    {
                                        WaitFor.Set();
                                    }
                                    catch (Exception) { }
                                }
                            }
                        });
                    }
                }

                try
                {
                    if (bWait)
                    {
                        WaitFor.WaitOne();
                    }
                    WaitFor.Close();
                }
                catch (Exception) { }
            }

            private bool OnFileUploaded_Internal(
                Action_StorageFileUploaded_CloudEventSchemaV1_0 _Action,
                Action<string> _ErrorMessageAction,
                Func<string, int, ModelDBEntry, Revision, int, bool> _SuccessCallback)
            {
                if (!FileEntry.SplitRelativeUrl(_Action.RelativeUrl,
                    out string OwnerModelName,
                    out int OwnerRevisionIndex,
                    out int StageNumber,
                    out bool bIsAProcessedFile,
                    out string _))
                {
                    _ErrorMessageAction?.Invoke("Error: PubSub_Handler->FileUploaded: SplitRelativeUrl has failed, url is: " + _Action.RelativeUrl);
                    return true; //It should return 200 anyways.
                }

                if (!CommonMethods.TryGettingModelID(
                        DatabaseService,
                        OwnerModelName,
                        out string OwnerModelID,
                        out BWebServiceResponse _FailureResponse,
                        _ErrorMessageAction))
                {
                    return false;
                }

                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), OwnerModelID, _ErrorMessageAction))
                {
                    return false; //Retry
                }
                try
                {
                    if (!CommonMethods.TryGettingAllInfo(
                        DatabaseService,
                        OwnerModelID,
                        OwnerRevisionIndex,
                        out ModelDBEntry ModelObject,
                        out Revision RevisionObject,
                        out int _,
                        out _FailureResponse,
                        _ErrorMessageAction))
                    {
                        if (_FailureResponse.StatusCode == BWebResponse.Error_NotFound_Code)
                        {
                            _ErrorMessageAction?.Invoke("Error: PubSub_Handler->FileUploaded: Model/revision does not exist: " + OwnerModelID + "->" + OwnerRevisionIndex);
                            return true; //Should return 200
                        }
                        return false; //DB Error - Retry
                    }

                    var bCallbackResult = _SuccessCallback?.Invoke(OwnerModelID, OwnerRevisionIndex, ModelObject, RevisionObject, StageNumber);
                    if (!bCallbackResult.HasValue) return false;
                    return bCallbackResult.Value;
                }
                finally
                {
                    Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), OwnerModelID, _ErrorMessageAction);
                }
            }

            private static bool SleepReturnTrue(int _MS) { Thread.Sleep(_MS); return true; }

            private bool FileUploaded(HttpListenerContext _Context, Action_StorageFileUploaded_CloudEventSchemaV1_0 _Action, Action<string> _ErrorMessageAction)
            {
                if (!OnFileUploaded_Internal(_Action, _ErrorMessageAction,
                    (string ModelID, int RevisionIndex, ModelDBEntry ModelObject, Revision RevisionObject, int StageNumber) =>
                    {
                        if (StageNumber == (int)EProcessStage.Stage0_FileUpload)
                        {
                            string ZipMainAssembly = "";

                            if (RevisionObject.FileEntry.ZipTypeMainAssemblyFileNameIfAny != null)
                            {
                                ZipMainAssembly = RevisionObject.FileEntry.ZipTypeMainAssemblyFileNameIfAny;
                            }

                            var RequestObject = new JObject()
                            {
                                ["bucketName"] = CadFileStorageBucketName,
                                ["rawFileRelativeUrl"] = _Action.RelativeUrl,
                                ["modelName"] = ModelObject.ModelName,
                                ["modelRevision"] = RevisionObject.RevisionIndex,
                                ["zipTypeMainAssemblyFileNameIfAny"] = ZipMainAssembly,
                                ["processStep"] = StageNumber,
                                ["filters"] = JsonConvert.SerializeObject(RevisionObject.FileEntry.Layers),
                                ["globalScale"] = RevisionObject.FileEntry.GlobalTransformOffset.UniformScale,
                                ["globalXOffset"] = RevisionObject.FileEntry.GlobalTransformOffset.LocationOffsetX,
                                ["globalYOffset"] = RevisionObject.FileEntry.GlobalTransformOffset.LocationOffsetY,
                                ["globalZOffset"] = RevisionObject.FileEntry.GlobalTransformOffset.LocationOffsetZ,
                                ["globalXRotation"] = RevisionObject.FileEntry.GlobalTransformOffset.RotationOffsetX,
                                ["globalYRotation"] = RevisionObject.FileEntry.GlobalTransformOffset.RotationOffsetY,
                                ["globalZRotation"] = RevisionObject.FileEntry.GlobalTransformOffset.RotationOffsetZ,
                                ["optimizationPreset"] = RevisionObject.FileEntry.OptimizationPreset,
                                ["mergeFinalLevel"] = RevisionObject.FileEntry.bMergeFinalLevel,
                                ["deleteDuplicates"] = RevisionObject.FileEntry.bDetectDuplicateMeshes
                            };

                            //TODO: Fix instabilities and uncomment below.
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
                                _ErrorMessageAction?.Invoke("Error: PubSub_Handler->RawFileUploaded: InterServicesRequest to start processing the raw file has failed. Error: Code: " + Result.ResponseCode + ", Content: " + Result.Content.String + ", given url: " + _Action.RelativeUrl);
                                return false; //Retry
                            }
                        }

                        if (StageNumber == (int)EProcessStage.Stage6_UnrealEngineConvertion)
                        {
                            RevisionObject.FileEntry.FileUploadProcessStage = (int)EUploadProcessStage.Uploaded_Processed;
                        }
                        else
                        {
                            RevisionObject.FileEntry.FileUploadProcessStage = (int)EUploadProcessStage.Uploaded_Processing;
                        }
                        RevisionObject.FileEntry.CurrentProcessStage = StageNumber;
                        RevisionObject.FileEntry.FileProcessedAtTime = Methods.ToISOString();
                        ModelObject.MRVLastUpdateTime = RevisionObject.FileEntry.FileProcessedAtTime;

                        var FinalSerializedModelObject = JObject.Parse(JsonConvert.SerializeObject(ModelObject));

                        Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                            _Context,
                            ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                            ModelDBEntry.KEY_NAME_MODEL_ID,
                            new BPrimitiveType(ModelID),
                            FinalSerializedModelObject);

                        Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelRevisionRawFileUploaded
                        (
                            ModelID,
                            RevisionIndex,
                            ModelObject.ModelOwnerUserID,
                            ModelObject.ModelSharedWithUserIDs,
                            ModelObject.ModelOwnerUserID,
                            FinalSerializedModelObject
                        ),
                        _ErrorMessageAction);

                        return true;

                    })) { return false; /* Retry */ }

                return true;
            }

            private bool DeleteFiles(HttpListenerContext _Context, Action_ModelRevisionFileEntryDeleteAll _Action, Action<string> _ErrorMessageAction)
            {
                //Note: User/Model entry might be deleted by this time.

                var Entry = JsonConvert.DeserializeObject<FileEntry>(_Action.Entry.ToString());

                Entry.DeleteAllFiles(_Context, CadFileStorageBucketName, _ErrorMessageAction);
                return true;
            }
        }
    }
}