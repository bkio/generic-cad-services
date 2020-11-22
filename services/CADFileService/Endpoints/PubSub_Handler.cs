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
using ServiceUtilities.Process.Procedure;
using ServiceUtilities.Process.RandomAccessFile;
using CADFileService.Endpoints.Structures;
using ServiceUtilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.IO;
using BWebServiceUtilities_GC;

namespace CADFileService.Endpoints
{
    partial class InternalCalls
    {
        internal class PubSub_To_CadFileService : PubSubServiceBaseTimeoutableDeliveryEnsurerUser
        {
            private readonly IBDatabaseServiceInterface DatabaseService;
            private readonly IBFileServiceInterface FileService;

            private readonly string CadFileStorageBucketName;
            private readonly string CadProcessServiceEndpoint;

            public PubSub_To_CadFileService(string _InternalCallPrivateKey, IBDatabaseServiceInterface _DatabaseService, IBFileServiceInterface _FileService, string _CadFileStorageBucketName, string _CadProcessServiceEndpoint) : base(_InternalCallPrivateKey)
            {
                DatabaseService = _DatabaseService;
                FileService = _FileService;
                CadFileStorageBucketName = _CadFileStorageBucketName;
                CadProcessServiceEndpoint = _CadProcessServiceEndpoint;
            }

            protected override bool Handle(HttpListenerContext _Context, ServiceUtilities.Action _Action, Action<string> _ErrorMessageAction = null)
            {
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
                else if (_Action.GetActionType() == Actions.EAction.ACTION_STORAGE_FILE_UPLOADED)
                {
                    var Casted = (Action_StorageFileUploaded)_Action;

                    if (Casted.RelativeUrl.StartsWith(FileEntry.RAW_FILE_FOLDER_PREFIX))
                        return RawFileUploaded(_Context, Casted, _ErrorMessageAction);
                    if (Casted.RelativeUrl.StartsWith(Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.HIERARCHY_RAF]))
                        return HierarchyRAFUploaded(_Context, Casted, _ErrorMessageAction);
                    if (Casted.RelativeUrl.StartsWith(Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.HIERARCHY_CF]))
                        return HierarchyCFUploaded(_Context, Casted, _ErrorMessageAction);
                    if (Casted.RelativeUrl.StartsWith(Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.GEOMETRY_RAF]))
                        return GeometryRAFUploaded(_Context, Casted, _ErrorMessageAction);
                    if (Casted.RelativeUrl.StartsWith(Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.GEOMETRY_CF]))
                        return GeometryCFUploaded(_Context, Casted, _ErrorMessageAction);
                    if (Casted.RelativeUrl.StartsWith(Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.METADATA_RAF]))
                        return MetadataRAFUploaded(_Context, Casted, _ErrorMessageAction);
                    if (Casted.RelativeUrl.StartsWith(Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.METADATA_CF]))
                        return MetadataCFUploaded(_Context, Casted, _ErrorMessageAction);
                    if (Casted.RelativeUrl.StartsWith(Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.UNREAL_HGM]))
                        return UnrealHGMUploaded(_Context, Casted, _ErrorMessageAction);
                    if (Casted.RelativeUrl.StartsWith(Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.UNREAL_HG]))
                        return UnrealHGUploaded(_Context, Casted, _ErrorMessageAction);
                    if (Casted.RelativeUrl.StartsWith(Constants.ProcessedFileType_FolderPrefix_Map[EProcessedFileType.UNREAL_H]))
                        return UnrealHUploaded(_Context, Casted, _ErrorMessageAction);
                    //Don't execute for Unreal_G because there is no one specific file for it but multiple
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
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), _Action.ModelID, _ErrorMessageAction))
                {
                    return false; //Retry
                }
                try
                {
                    if (!CommonMethods.TryGettingAllInfo(
                        DatabaseService,
                        _Action.ModelID,
                        _Action.RevisionIndex,
                        out ModelDBEntry ModelObject,
                        out Revision RevisionObject,
                        out int _,
                        out BWebServiceResponse _FailureResponse,
                        _ErrorMessageAction))
                    {
                        if (_FailureResponse.StatusCode == BWebResponse.Error_NotFound_Code)
                        {
                            _ErrorMessageAction?.Invoke($"Error: PubSub_Handler->UpdateModelBatchProcessFailed: Model/revision does not exist: {_Action.ModelID}->{_Action.RevisionIndex}");
                            return true; //Should return 200
                        }
                        return false; //DB Error - Retry
                    }

                    RevisionObject.FileEntry.FileProcessStage = (int)Constants.EProcessStage.Uploaded_ProcessFailed;
                    ModelObject.MRVLastUpdateTime = CommonMethods.GetTimeAsCreationTime();

                    Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                        _Context,
                        ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                        ModelDBEntry.KEY_NAME_MODEL_ID,
                        new BPrimitiveType(_Action.ModelID),
                        JObject.Parse(JsonConvert.SerializeObject(ModelObject)));

                    return true;
                }
                finally
                {
                    Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), _Action.ModelID, _ErrorMessageAction);
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
                string _CallerMethod,
                Action_StorageFileUploaded _Action,
                EProcessedFileType _ContinueIfRelativeUrlIsA,
                Action<string> _ErrorMessageAction,
                Func<string, int, ModelDBEntry, Revision, bool> _SuccessCallback)
            {
                if (!FileEntry.SplitRelativeUrl(_Action.RelativeUrl,
                    out string OwnerModelID,
                    out int OwnerRevisionIndex,
                    out bool bIsAProcessedFile,
                    out EProcessedFileType ProcessedFileType_IfProcessed,
                    out string RawExtension_IfRaw))
                {
                    _ErrorMessageAction?.Invoke("Error: PubSub_Handler->RawFileUploaded: SplitRelativeUrl has failed, url is: " + _Action.RelativeUrl);
                    return true; //It should return 200 anyways.
                }

                if ((_ContinueIfRelativeUrlIsA == EProcessedFileType.NONE_OR_RAW && !bIsAProcessedFile)
                    || bIsAProcessedFile && _ContinueIfRelativeUrlIsA == ProcessedFileType_IfProcessed)
                {
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
                            out BWebServiceResponse _FailureResponse,
                            _ErrorMessageAction))
                        {
                            if (_FailureResponse.StatusCode == BWebResponse.Error_NotFound_Code)
                            {
                                _ErrorMessageAction?.Invoke("Error: PubSub_Handler->" + _CallerMethod + ": Model/revision does not exist: " + OwnerModelID + "->" + OwnerRevisionIndex);
                                return true; //Should return 200
                            }
                            return false; //DB Error - Retry
                        }

                        var bCallbackResult = _SuccessCallback?.Invoke(OwnerModelID, OwnerRevisionIndex, ModelObject, RevisionObject);
                        if (!bCallbackResult.HasValue) return false;
                        return bCallbackResult.Value;
                    }
                    finally
                    {
                        Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), OwnerModelID, _ErrorMessageAction);
                    }
                }
                else
                {
                    _ErrorMessageAction?.Invoke("Error: PubSub_Handler->" + _CallerMethod + ": Input url is not as expected: " + _Action.RelativeUrl + ", expected: " + _ContinueIfRelativeUrlIsA);
                }

                return true;
            }
            private bool CheckIfAllProcessedFilesUploaded(out bool _bAllUploaded, Revision _RevisionObject, Action<string> _ErrorMessageAction)
            {
                _bAllUploaded = false;

                if (!FileService.CheckFileExistence(CadFileStorageBucketName, _RevisionObject.FileEntry.HierarchyRAFRelativeUrl, out bool bExist, _ErrorMessageAction))
                    return false; //Retry
                if (!bExist)
                    return true;

                if (!FileService.CheckFileExistence(CadFileStorageBucketName, _RevisionObject.FileEntry.HierarchyCFRelativeUrl, out bExist, _ErrorMessageAction))
                    return false; //Retry
                if (!bExist)
                    return true;

                if (!FileService.CheckFileExistence(CadFileStorageBucketName, _RevisionObject.FileEntry.GeometryRAFRelativeUrl, out bExist, _ErrorMessageAction))
                    return false; //Retry
                if (!bExist)
                    return true;

                if (!FileService.CheckFileExistence(CadFileStorageBucketName, _RevisionObject.FileEntry.GeometryCFRelativeUrl, out bExist, _ErrorMessageAction))
                    return false; //Retry
                if (!bExist)
                    return true;

                if (!FileService.CheckFileExistence(CadFileStorageBucketName, _RevisionObject.FileEntry.MetadataRAFRelativeUrl, out bExist, _ErrorMessageAction))
                    return false; //Retry
                if (!bExist)
                    return true;

                if (!FileService.CheckFileExistence(CadFileStorageBucketName, _RevisionObject.FileEntry.MetadataCFRelativeUrl, out bExist, _ErrorMessageAction))
                    return false; //Retry
                if (!bExist)
                    return true;

                if (!FileService.CheckFileExistence(CadFileStorageBucketName, _RevisionObject.FileEntry.UnrealHGMRelativeUrl, out bExist, _ErrorMessageAction))
                    return false; //Retry
                if (!bExist)
                    return true;

                if (!FileService.CheckFileExistence(CadFileStorageBucketName, _RevisionObject.FileEntry.UnrealHGRelativeUrl, out bExist, _ErrorMessageAction))
                    return false; //Retry
                if (!bExist)
                    return true;

                if (!FileService.CheckFileExistence(CadFileStorageBucketName, _RevisionObject.FileEntry.UnrealHRelativeUrl, out bExist, _ErrorMessageAction))
                    return false; //Retry
                if (!bExist)
                    return true;

                _bAllUploaded = true;
                return true;
            }
            private void UpdateDatabaseEntry_AllProcessedFilesUploaded(HttpListenerContext _Context, string _ModelID, ModelDBEntry _ModelObject, Revision _RevisionObject, Action<string> _ErrorMessageAction)
            {
                _RevisionObject.FileEntry.FileProcessStage = (int)Constants.EProcessStage.Uploaded_Processed;
                _ModelObject.MRVLastUpdateTime = CommonMethods.GetTimeAsCreationTime();

                Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                    _Context,
                    ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                    ModelDBEntry.KEY_NAME_MODEL_ID,
                    new BPrimitiveType(_ModelID),
                    JObject.Parse(JsonConvert.SerializeObject(_ModelObject)));
            }
            private static bool SleepReturnTrue(int _MS) { Thread.Sleep(_MS); return true; }

            private bool RawFileUploaded(HttpListenerContext _Context, Action_StorageFileUploaded _Action, Action<string> _ErrorMessageAction)
            {
                if (!OnFileUploaded_Internal("RawFileUploaded", _Action, EProcessedFileType.NONE_OR_RAW, _ErrorMessageAction,
                    (string ModelID, int RevisionIndex, ModelDBEntry ModelObject, Revision RevisionObject) =>
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
                            ["zipTypeMainAssemblyFileNameIfAny"] = ZipMainAssembly
                        };

                        //TODO: Fix instabilities and uncomment below.
                        int TryCount = 0;
                        BWebUtilities_GC_CloudRun.InterServicesRequestResponse Result;
                        do
                        {
                            GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

                            Result = BWebUtilities_GC_CloudRun.InterServicesRequest(new BWebUtilities_GC_CloudRun.InterServicesRequestRequest()
                            {
                                DestinationServiceUrl = CadProcessServiceEndpoint + "/3d/process/start",
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

                        RevisionObject.FileEntry.FileProcessStage = (int)Constants.EProcessStage.Uploaded_Processing;
                        RevisionObject.FileEntry.FileProcessedAtTime = CommonMethods.GetTimeAsCreationTime();
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

            private bool HierarchyRAFUploaded(HttpListenerContext _Context, Action_StorageFileUploaded _Action, Action<string> _ErrorMessageAction)
            {
                bool bProceedToFindRoot = false;

                string ModelID = null;
                int RevisionIndex = -1;
                if (!OnFileUploaded_Internal("HierarchyRAFUploaded", _Action, EProcessedFileType.HIERARCHY_RAF, _ErrorMessageAction,
                    (string _ModelID, int _RevisionIndex, ModelDBEntry _ModelObject, Revision _RevisionObject) =>
                {
                    if (!CheckIfAllProcessedFilesUploaded(out bool bAllUploaded, _RevisionObject, _ErrorMessageAction))
                        return false; //Retry
                    if (bAllUploaded)
                        UpdateDatabaseEntry_AllProcessedFilesUploaded(_Context, _ModelID, _ModelObject, _RevisionObject, _ErrorMessageAction);

                    bProceedToFindRoot = true;

                    ModelID = _ModelID;
                    RevisionIndex = _RevisionIndex;
                    return true;

                })) { return false; /* Retry */ }

                //ModelDBEntry is not locked here anymore

                if (bProceedToFindRoot)
                {
                    var bSuccess = new BValue<bool>(true);
                    var RootNodeID = new BValue<ulong>(0);

                    Thread CancellableThread = null;
                    try
                    {
                        var WaitFor = new ManualResetEvent(false);

                        CancellableThread = new Thread(() =>
                        {
                            //TODO: Refactor for XStreamReader changes. Its no longer a stream and processes a given stream. Also check compensate for file that could be compressed or not
                            MemoryStream MemStream = new MemoryStream();
                            bool DownloadComplete = false;

                            using var XReader = new XStreamReader(ENodeType.Hierarchy, MemStream,
                                (uint FileVersionSDK) =>
                                {
                                },
                                (Node CurrentNode) =>
                                {
                                    if ((CurrentNode as HierarchyNode).ParentID == Node.UNDEFINED_ID)
                                    {
                                        RootNodeID.Set((CurrentNode as HierarchyNode).UniqueID);

                                        try
                                        {
                                            WaitFor.Set();
                                        }
                                        catch (Exception) { }

                                        CancellableThread.Abort();
                                    }
                                }, EDeflateCompression.DoNotCompress);
                            {
                                BTaskWrapper.Run(() =>
                                {
                                    while (!DownloadComplete)
                                    {
                                        XReader.Process();
                                    }
                                });
                            }

                            if (!FileService.DownloadFile(CadFileStorageBucketName, _Action.RelativeUrl, new BStringOrStream(MemStream, 0), _ErrorMessageAction))
                            {
                                if (RootNodeID.Get() == 0 && FileService.CheckFileExistence(CadFileStorageBucketName, _Action.RelativeUrl, out bool bExists, _ErrorMessageAction) && bExists)
                                {
                                    bSuccess.Set(false); //Retry
                                }
                            }
                            DownloadComplete = true;
                            try
                            {
                                WaitFor.Set();
                            }
                            catch (Exception) { }
                        });
                        CancellableThread.Start();

                        try
                        {
                            WaitFor.WaitOne();
                            WaitFor.Close();
                        }
                        catch (Exception) { }
                    }
                    catch (Exception e)
                    {
                        if (!(e is ThreadAbortException))
                        {
                            _ErrorMessageAction?.Invoke("Error: PubSub_Handler->HierarchyRAFUploaded: bSuccess: " + bSuccess.Get() + ", RootNodeID: " + RootNodeID.Get() + ", Message: " + e.Message + ", trace: " + e.StackTrace);
                        }
                    }

                    if (!bSuccess.Get()) return false; //Retry

                    //Lock the ModelDBEntry again
                    if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), ModelID, _ErrorMessageAction))
                    {
                        return false; //Retry
                    }

                    if (!CommonMethods.TryGettingAllInfo(
                        DatabaseService,
                        ModelID,
                        RevisionIndex,
                        out ModelDBEntry ModelObject,
                        out Revision RevisionObject,
                        out int _,
                        out BWebServiceResponse _FailureResponse,
                        _ErrorMessageAction))
                    {
                        if (_FailureResponse.StatusCode == BWebResponse.Error_NotFound_Code)
                        {
                            _ErrorMessageAction?.Invoke("Error: PubSub_Handler->HierarchyRAFUploaded: Model/revision does not exist: " + ModelID + "->" + RevisionIndex);
                            return true; //Should return 200
                        }
                        return false; //DB Error - Retry
                    }

                    RevisionObject.FileEntry.ProcessedFilesRootNodeID = RootNodeID.Get();
                    ModelObject.MRVLastUpdateTime = CommonMethods.GetTimeAsCreationTime();

                    Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                        _Context,
                        ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                        ModelDBEntry.KEY_NAME_MODEL_ID,
                        new BPrimitiveType(ModelID),
                        JObject.Parse(JsonConvert.SerializeObject(ModelObject)));

                    Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), ModelID, _ErrorMessageAction);
                }

                return true;
            }
            private bool HierarchyCFUploaded(HttpListenerContext _Context, Action_StorageFileUploaded _Action, Action<string> _ErrorMessageAction)
            {
                if (!OnFileUploaded_Internal("HierarchyCFUploaded", _Action, EProcessedFileType.HIERARCHY_CF, _ErrorMessageAction,
                    (string ModelID, int RevisionIndex, ModelDBEntry ModelObject, Revision RevisionObject) =>
                {
                    if (!CheckIfAllProcessedFilesUploaded(out bool bAllUploaded, RevisionObject, _ErrorMessageAction))
                        return false; //Retry
                    if (bAllUploaded)
                        UpdateDatabaseEntry_AllProcessedFilesUploaded(_Context, ModelID, ModelObject, RevisionObject, _ErrorMessageAction);
                    return true;

                })) { return false; /* Retry */ }

                return true;
            }

            private bool GeometryRAFUploaded(HttpListenerContext _Context, Action_StorageFileUploaded _Action, Action<string> _ErrorMessageAction)
            {
                if (!OnFileUploaded_Internal("GeometryRAFUploaded", _Action, EProcessedFileType.GEOMETRY_RAF, _ErrorMessageAction,
                    (string ModelID, int RevisionIndex, ModelDBEntry ModelObject, Revision RevisionObject) =>
                {
                    if (!CheckIfAllProcessedFilesUploaded(out bool bAllUploaded, RevisionObject, _ErrorMessageAction))
                        return false; //Retry
                    if (bAllUploaded)
                        UpdateDatabaseEntry_AllProcessedFilesUploaded(_Context, ModelID, ModelObject, RevisionObject, _ErrorMessageAction);
                    return true;

                })) { return false; /* Retry */ }

                return true;
            }
            private bool GeometryCFUploaded(HttpListenerContext _Context, Action_StorageFileUploaded _Action, Action<string> _ErrorMessageAction)
            {
                if (!OnFileUploaded_Internal("GeometryCFUploaded", _Action, EProcessedFileType.GEOMETRY_CF, _ErrorMessageAction,
                (string ModelID, int RevisionIndex, ModelDBEntry ModelObject, Revision RevisionObject) =>
                {
                    if (!CheckIfAllProcessedFilesUploaded(out bool bAllUploaded, RevisionObject, _ErrorMessageAction))
                        return false; //Retry
                    if (bAllUploaded)
                        UpdateDatabaseEntry_AllProcessedFilesUploaded(_Context, ModelID, ModelObject, RevisionObject, _ErrorMessageAction);
                    return true;

                })) { return false; /* Retry */ }

                return true;
            }

            private bool MetadataRAFUploaded(HttpListenerContext _Context, Action_StorageFileUploaded _Action, Action<string> _ErrorMessageAction)
            {
                bool bProceedToParseMetadata = false;
                string OwnerUserID = null;

                if (!OnFileUploaded_Internal("MetadataRAFUploaded", _Action, EProcessedFileType.METADATA_RAF, _ErrorMessageAction,
                (string ModelID, int RevisionIndex, ModelDBEntry ModelObject, Revision RevisionObject) =>
                {
                    if (!CheckIfAllProcessedFilesUploaded(out bool bAllUploaded, RevisionObject, _ErrorMessageAction))
                        return false; //Retry
                    if (bAllUploaded)
                        UpdateDatabaseEntry_AllProcessedFilesUploaded(_Context, ModelID, ModelObject, RevisionObject, _ErrorMessageAction);

                    OwnerUserID = ModelObject.ModelOwnerUserID;
                    bProceedToParseMetadata = true;
                    return true;

                })) { return false; /* Retry */ }

                //ModelDBEntry is not locked here anymore

                if (bProceedToParseMetadata)
                {
                    return Add_Remove_AttributesFromMetadataRAF(Controller_AttributeTables.EAddRemove.Add, _Action.RelativeUrl, OwnerUserID, _ErrorMessageAction);
                }

                return true;
            }
            private bool MetadataCFUploaded(HttpListenerContext _Context, Action_StorageFileUploaded _Action, Action<string> _ErrorMessageAction)
            {
                if (!OnFileUploaded_Internal("MetadataCFUploaded", _Action, EProcessedFileType.METADATA_CF, _ErrorMessageAction,
                (string ModelID, int RevisionIndex, ModelDBEntry ModelObject, Revision RevisionObject) =>
                {
                    if (!CheckIfAllProcessedFilesUploaded(out bool bAllUploaded, RevisionObject, _ErrorMessageAction))
                        return false; //Retry
                    if (bAllUploaded)
                        UpdateDatabaseEntry_AllProcessedFilesUploaded(_Context, ModelID, ModelObject, RevisionObject, _ErrorMessageAction);
                    return true;

                })) { return false; /* Retry */ }

                return true;
            }

            private bool UnrealHGMUploaded(HttpListenerContext _Context, Action_StorageFileUploaded _Action, Action<string> _ErrorMessageAction)
            {
                if (!OnFileUploaded_Internal("UnrealHGMUploaded", _Action, EProcessedFileType.UNREAL_HGM, _ErrorMessageAction,
                (string ModelID, int RevisionIndex, ModelDBEntry ModelObject, Revision RevisionObject) =>
                {
                    if (!CheckIfAllProcessedFilesUploaded(out bool bAllUploaded, RevisionObject, _ErrorMessageAction))
                        return false; //Retry
                    if (bAllUploaded)
                        UpdateDatabaseEntry_AllProcessedFilesUploaded(_Context, ModelID, ModelObject, RevisionObject, _ErrorMessageAction);
                    return true;

                })) { return false; /* Retry */ }

                return true;
            }
            private bool UnrealHGUploaded(HttpListenerContext _Context, Action_StorageFileUploaded _Action, Action<string> _ErrorMessageAction)
            {
                if (!OnFileUploaded_Internal("UnrealHGUploaded", _Action, EProcessedFileType.UNREAL_HG, _ErrorMessageAction,
                (string ModelID, int RevisionIndex, ModelDBEntry ModelObject, Revision RevisionObject) =>
                {
                    if (!CheckIfAllProcessedFilesUploaded(out bool bAllUploaded, RevisionObject, _ErrorMessageAction))
                        return false; //Retry
                    if (bAllUploaded)
                        UpdateDatabaseEntry_AllProcessedFilesUploaded(_Context, ModelID, ModelObject, RevisionObject, _ErrorMessageAction);
                    return true;

                })) { return false; /* Retry */ }

                return true;
            }

            private bool UnrealHUploaded(HttpListenerContext _Context, Action_StorageFileUploaded _Action, Action<string> _ErrorMessageAction)
            {
                if (!OnFileUploaded_Internal("UnrealHUploaded", _Action, EProcessedFileType.UNREAL_H, _ErrorMessageAction,
                (string ModelID, int RevisionIndex, ModelDBEntry ModelObject, Revision RevisionObject) =>
                {
                    if (!CheckIfAllProcessedFilesUploaded(out bool bAllUploaded, RevisionObject, _ErrorMessageAction))
                        return false; //Retry
                    if (bAllUploaded)
                        UpdateDatabaseEntry_AllProcessedFilesUploaded(_Context, ModelID, ModelObject, RevisionObject, _ErrorMessageAction);
                    return true;

                })) { return false; /* Retry */ }

                return true;
            }

            private bool DeleteFiles(HttpListenerContext _Context, Action_ModelRevisionFileEntryDeleteAll _Action, Action<string> _ErrorMessageAction)
            {
                //Note: User/Model entry might be deleted by this time.

                var Entry = JsonConvert.DeserializeObject<FileEntry>(_Action.Entry.ToString());
                if (!Add_Remove_AttributesFromMetadataRAF(Controller_AttributeTables.EAddRemove.Remove, Entry.MetadataRAFRelativeUrl, _Action.UserID, _ErrorMessageAction))
                {
                    return false; //Retry
                }

                Entry.DeleteAllFiles(_Context, CadFileStorageBucketName, _ErrorMessageAction);
                return true;
            }

            private bool Add_Remove_AttributesFromMetadataRAF(Controller_AttributeTables.EAddRemove _AddOrRemove, string _MetadataRAFRelativeUrl, string _UserID, Action<string> _ErrorMessageAction)
            {
                FileEntry.SplitRelativeUrl(_MetadataRAFRelativeUrl, out string ModelID, out int RevisionIndex, out bool _, out EProcessedFileType _, out string _);

                //TODO: Refactor for XStreamReader changes. Its no longer a stream and processes a given stream. Also check compensate for file that could be compressed or not
                MemoryStream MemStream = new MemoryStream();
                bool DownloadComplete = false;

                using var XReader = new XStreamReader(ENodeType.Hierarchy, MemStream,
                    (uint FileVersionSDK) =>
                    {
                    },
                    (Node CurrentNode) =>
                    {
                        try
                        {
                            var CastedNode = CurrentNode as MetadataNode;

                            var CompiledMetadataNode = new Metadata();
                            var MetadataID = CastedNode.UniqueID;
                            var ParsedMetadata = JObject.Parse(CastedNode.Metadata);

                            foreach (var Pair in ParsedMetadata)
                            {
                                CompiledMetadataNode.MetadataKey = Pair.Key;
                                if (Pair.Value.Type == JTokenType.String)
                                {
                                    CompiledMetadataNode.MetadataValues.Add((string)Pair.Value);
                                }
                                else if (Pair.Value.Type == JTokenType.Array)
                                {
                                    var ValueAsArray = (JArray)Pair.Value;
                                    foreach (var ValChild in ValueAsArray)
                                    {
                                        if (ValChild.Type == JTokenType.String)
                                        {
                                            CompiledMetadataNode.MetadataValues.Add((string)ValChild);
                                        }
                                        else if (ValChild.Type == JTokenType.Object)
                                        {
                                            CompiledMetadataNode.MetadataValues.Add(((JObject)ValChild).ToString());
                                        }
                                        else
                                        {
                                            CompiledMetadataNode.MetadataValues.Add(ValChild.ToString());
                                        }
                                    }
                                }
                                else if (Pair.Value.Type == JTokenType.Object)
                                {
                                    CompiledMetadataNode.MetadataValues.Add(((JObject)Pair.Value).ToString());
                                }
                                else
                                {
                                    CompiledMetadataNode.MetadataValues.Add(Pair.Value.ToString());
                                }
                            }

                            //This gets clearance internally
                            Controller_AttributeTables.Get().AddRemoveMetadataSets_AttributesTables(
                                InnerDeliveryEnsurerUserProcessor, _UserID,
                                Controller_AttributeTables.MetadataLocator.ItIsRevisionMetadata(ModelID, RevisionIndex, MetadataID),
                                new List<Metadata>() { CompiledMetadataNode },
                                _AddOrRemove,
                                Controller_AttributeTables.EKillProcedureIfGetClearanceFails.No,
                                out BWebServiceResponse _, _ErrorMessageAction);
                        }
                        catch (Exception e)
                        {
                            _ErrorMessageAction?.Invoke("Error: PubSub_Handler->MetadataRAFUploaded->OnNodeParsed, during parsing metadata: " + _MetadataRAFRelativeUrl + ", message: " + e.Message + ", node: " + CurrentNode?.UniqueID + ", trace: " + e.StackTrace);
                        }
                    }, EDeflateCompression.DoNotCompress);
                {
                    BTaskWrapper.Run(() =>
                    {
                        while (!DownloadComplete)
                        {
                            XReader.Process();
                        }
                    });
                }

                if (FileService.CheckFileExistence(CadFileStorageBucketName, _MetadataRAFRelativeUrl, out bool bExist, _ErrorMessageAction) && bExist)
                {
                    if (!FileService.DownloadFile(CadFileStorageBucketName, _MetadataRAFRelativeUrl, new BStringOrStream(MemStream, 0), _ErrorMessageAction))
                    {
                        DownloadComplete = true;
                        return false; //Retry
                    }
                }

                DownloadComplete = true;
                return true;
            }
        }
    }
}