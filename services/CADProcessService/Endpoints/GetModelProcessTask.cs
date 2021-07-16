/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceUtilities;
using ServiceUtilities.All;
using ServiceUtilities.Common;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using System;
using System.Collections.Generic;
using System.Net;

namespace CADProcessService.Endpoints
{
    public class GetModelProcessTask : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly IBFileServiceInterface FileService;

        public GetModelProcessTask(
            IBFileServiceInterface _FileService,
            IBDatabaseServiceInterface _DatabaseService) : base()
        {
            FileService = _FileService;
            DatabaseService = _DatabaseService;
        }

        public override BWebServiceResponse OnRequest_Interruptable_DeliveryEnsurerUser(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            GetTracingService()?.On_FromServiceToService_Received(_Context, _ErrorMessageAction);

            var Result = OnRequest_Internal(_Context, _ErrorMessageAction);

            GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse OnRequest_Internal(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            var RequestedVirtualMachineId = RestfulUrlParameters["fetch_task"];

            _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateFileConversionEntry-> Received GetNewTask Request: {RequestedVirtualMachineId}");

            BWebServiceResponse Response = BWebResponse.NotFound("No task was found to process");

            if (!DatabaseService.GetItem(
                    WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                    WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                    new BPrimitiveType(RequestedVirtualMachineId),
                    WorkerVMListDBEntry.Properties,
                    out JObject _ReturnObject, _ErrorMessageAction
                    ) || _ReturnObject == null)
            {
                Response = BWebResponse.InternalError("Worker vm list database error.");
            }

            WorkerVMListDBEntry VirtualMachineEntry = _ReturnObject.ToObject<WorkerVMListDBEntry>();

            if (VirtualMachineEntry != null)
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), "GETTASK", _ErrorMessageAction))
                {
                    return BWebResponse.InternalError($"Failed to get access to database record");
                }

                if (!DatabaseService.GetItem(
                    FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                    FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                    new BPrimitiveType(VirtualMachineEntry.ProcessId),
                    FileConversionDBEntry.Properties,
                    out JObject ConversionObject
                    ))
                {
                    _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateFileConversionEntry-> Failed to get record from db. ModelId: {VirtualMachineEntry.ProcessId}");
                }

                if (ConversionObject != null)
                {
                    FileConversionDBEntry _FileConversionEntry = ConversionObject.ToObject<FileConversionDBEntry>();

                    if (_FileConversionEntry.ConversionStatus == (int)EInternalProcessStage.Queued)
                    {
                        Response = UpdateFileConversionEntry(_FileConversionEntry, VirtualMachineEntry.ProcessId, _ErrorMessageAction);
                    }
                    else if (_FileConversionEntry.ConversionStatus == (int)EInternalProcessStage.ProcessComplete || _FileConversionEntry.ConversionStatus == (int)EInternalProcessStage.ProcessFailed)
                    {
                        if (DatabaseService.ScanTable(FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), out List<JObject> ConversionItems, _ErrorMessageAction))
                        {
                            if (ConversionItems != null)
                            {
                                foreach (var ConvertItem in ConversionItems)
                                {
                                    FileConversionDBEntry Entry = ConvertItem.ToObject<FileConversionDBEntry>();
                                    string _ModelId = (string)ConvertItem[FileConversionDBEntry.KEY_NAME_CONVERSION_ID];

                                    if (Entry.ConversionStatus == (int)EInternalProcessStage.Queued)
                                    {
                                        Response = UpdateFileConversionEntry(Entry, _ModelId, _ErrorMessageAction);
                                        UpdateWorkerVMEntry(RequestedVirtualMachineId, _ModelId, VirtualMachineEntry, Entry, _ErrorMessageAction);
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateFileConversionEntry-> There is no records returned scan table in FILE_CONVERSIONS_TABLE.");
                            }
                        }
                        else
                        {
                            _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateFileConversionEntry-> Scan table has returned false for FILE_CONVERSIONS_TABLE.");
                        }
                    }
                    else
                    {
                        _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateFileConversionEntry-> Received FileConversionEntry status is [Processing].");
                    }
                }
                else
                {
                    _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateFileConversionEntry-> There is no record with given vm id {RequestedVirtualMachineId} in FILE_CONVERSIONS_TABLE");
                }

                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), "GETTASK", _ErrorMessageAction);
            }
            else
            {
                _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateFileConversionEntry-> There is no record with given vm id {RequestedVirtualMachineId} in WORKERS_VM_LIST_TABLE");
            }

            return Response;
        }

        private BWebServiceResponse UpdateFileConversionEntry(FileConversionDBEntry _FileConvertionEntry, string _ModelId, Action<string> _ErrorMessageAction)
        {
            _FileConvertionEntry.ConversionStatus = (int)EInternalProcessStage.Processing;

            if (!DatabaseService.UpdateItem(
                FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                new BPrimitiveType(_ModelId),
                JObject.Parse(JsonConvert.SerializeObject(_FileConvertionEntry)),
                out JObject _, EBReturnItemBehaviour.DoNotReturn,
                null,
                _ErrorMessageAction))
            {
                _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateFileConversionEntry-> Experienced a Database error.");
                return BWebResponse.InternalError("Experienced a Database error");
            }

            ModelProcessTask Task = new ModelProcessTask();
            Task.CullingThresholds = _FileConvertionEntry.CullingThresholds;
            Task.GlobalScale = _FileConvertionEntry.GlobalScale;
            Task.GlobalXOffset = _FileConvertionEntry.GlobalXOffset;
            Task.GlobalXRotation = _FileConvertionEntry.GlobalXRotation;
            Task.GlobalYOffset = _FileConvertionEntry.GlobalYOffset;
            Task.GlobalYRotation = _FileConvertionEntry.GlobalYRotation;
            Task.GlobalZOffset = _FileConvertionEntry.GlobalZOffset;
            Task.GlobalZRotation = _FileConvertionEntry.GlobalZRotation;
            Task.LevelThresholds = _FileConvertionEntry.LevelThresholds;
            Task.LodParameters = _FileConvertionEntry.LodParameters;
            Task.ModelName = _FileConvertionEntry.ModelName;
            Task.ModelRevision = _FileConvertionEntry.ModelRevision;
            Task.ProcessStep = _FileConvertionEntry.ConversionStage;
            Task.ConversionId = _ModelId;
            Task.ZipMainAssemblyFileNameIfAny = _FileConvertionEntry.ZipMainAssemblyFileNameIfAny;
            Task.CustomPythonScript = _FileConvertionEntry.CustomPythonScript;
            Task.Layers = _FileConvertionEntry.Layers;
            Task.MergingParts = _FileConvertionEntry.MergingParts;

            var DeploymentBranchName = Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithUnderscore();
            FileService.CreateSignedURLForDownload(out string _StageDownloadUrl, _FileConvertionEntry.BucketName, $"{DeploymentBranchName}/{_ModelId}/{_FileConvertionEntry.ModelRevision}/stages/{_FileConvertionEntry.ConversionStage}/files.zip", 180, _ErrorMessageAction);
            Task.StageDownloadUrl = _StageDownloadUrl;

            string TaskAsString = JsonConvert.SerializeObject(Task);
            _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateFileConversionEntry-> Returned task: {TaskAsString}");
            return new BWebServiceResponse(200, new BStringOrStream(TaskAsString));
        }

        private bool UpdateWorkerVMEntry(
            string _RequestedVirtualMachineId,
            string _ModelId,
            WorkerVMListDBEntry _VirtualMachineEntry,
            FileConversionDBEntry _FileConversionEntry,
            Action<string> _ErrorMessageAction)
        {
            try
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), _RequestedVirtualMachineId, _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateDBVirtualMachineAvailability-> Failed to update db for because atomic db operation has been failed.");
                    return false;
                }

                _VirtualMachineEntry.CurrentProcessStage = _FileConversionEntry.ConversionStage;
                _VirtualMachineEntry.LastKnownProcessStatus = 0;
                _VirtualMachineEntry.LastKnownProcessStatusInfo = "Init";
                _VirtualMachineEntry.ProcessStartDate = Methods.ToISOString();
                _VirtualMachineEntry.ProcessEndDate = "";
                _VirtualMachineEntry.VMStatus = (int)EVMStatus.Busy;
                _VirtualMachineEntry.ModelName = _FileConversionEntry.ModelName;
                _VirtualMachineEntry.ProcessId = _ModelId;
                _VirtualMachineEntry.RevisionIndex = _FileConversionEntry.ModelRevision;

                if (!DatabaseService.UpdateItem(
                        WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                        WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                        new BPrimitiveType(_RequestedVirtualMachineId),
                        JObject.Parse(JsonConvert.SerializeObject(_VirtualMachineEntry)),
                        out JObject _, EBReturnItemBehaviour.DoNotReturn,
                        null,
                        _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateDBVirtualMachineAvailability-> Failed to update worker-vm-list table");
                    return false;
                }
            }
            catch (System.Exception ex)
            {
                _ErrorMessageAction?.Invoke($"GetModelProcessTask:UpdateDBVirtualMachineAvailability-> Failed to update worker-vm-list table. Error: {ex.Message}. Trace: {ex.StackTrace}");
                return false;
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), _RequestedVirtualMachineId, _ErrorMessageAction);
            }
            return true;
        }
    }
}
