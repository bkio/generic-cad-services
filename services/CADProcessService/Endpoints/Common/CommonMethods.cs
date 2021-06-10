/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using BWebServiceUtilities;
using BCloudServiceUtilities;
using CADProcessService.Endpoints.Structures;
using ServiceUtilities;
using CADProcessService.Endpoints.Controllers;
using ServiceUtilities.Common;
using Newtonsoft.Json;
using BCommonUtilities;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;

namespace CADProcessService.Endpoints.Common
{
    public class CommonMethods
    {
        public static bool StopVirtualMachine(
            IBVMServiceInterface _VirtualMachineService,
            IBDatabaseServiceInterface _DatabaseService,
            WebServiceBaseTimeoutableProcessor _InnerProcessor,
            WorkerVMListDBEntry _VirtualMachineEntry,
            string _RequestedVirtualMachineId, 
            Action<string> _ErrorMessageAction, 
            out BWebServiceResponse _FailureResponse)
        {
            string VirtualMachineName = _VirtualMachineEntry.VMName;
            _FailureResponse = BWebResponse.InternalError("");

            if (!_VirtualMachineService.StopInstances(new string[] { VirtualMachineName },
                () =>
                {
                    if (!UpdateDBVirtualMachineAvailability(_DatabaseService, _InnerProcessor, _RequestedVirtualMachineId, _ErrorMessageAction, VirtualMachineName, _VirtualMachineEntry))
                    {
                        _ErrorMessageAction?.Invoke($"Failed to update worker-vm-list database for virtual machine [{VirtualMachineName}]");
                    }

                    if (!UpdateDBProcessHistory(_DatabaseService, _InnerProcessor, _VirtualMachineEntry, _ErrorMessageAction))
                    {
                        _ErrorMessageAction?.Invoke($"Failed to update process-history database for virtual machine [{VirtualMachineName}]");
                    }

                    Controller_BatchProcess.Get().BroadcastBatchProcessAction(new Action_BatchProcessFailed()
                    {
                        ModelName = _VirtualMachineEntry.ModelName,
                        RevisionIndex = _VirtualMachineEntry.RevisionIndex
                    }, _ErrorMessageAction);
                },
                () =>
                {
                    _ErrorMessageAction?.Invoke($"Failed to stop virtual machine [{VirtualMachineName}]");
                }, _ErrorMessageAction))
            {
                _FailureResponse = BWebResponse.InternalError($"Failed to stop virtual machine [{VirtualMachineName}]");
                return false;
            }

            return true;
        }

        private static bool UpdateDBVirtualMachineAvailability(
            IBDatabaseServiceInterface _DatabaseService,
            WebServiceBaseTimeoutableProcessor _InnerProcessor,
            string _RequestedVirtualMachineId,
            Action<string> _ErrorMessageAction,
            string VirtualMachineName,
            WorkerVMListDBEntry _VirtualMachineEntry)
        {
            try
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(_InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), _RequestedVirtualMachineId, _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke($"Failed to update db for [{VirtualMachineName}] because atomic db operation has been failed.");
                    return false;
                }

                _VirtualMachineEntry.VMStatus = (int)EVMStatus.Available;
                _VirtualMachineEntry.LastKnownProcessStatus = (int)EProcessStatus.Canceled;

                if (!_DatabaseService.UpdateItem(
                    WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                    WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                    new BPrimitiveType(_RequestedVirtualMachineId),
                    JObject.Parse(JsonConvert.SerializeObject(_VirtualMachineEntry)),
                    out JObject _, EBReturnItemBehaviour.DoNotReturn,
                    _DatabaseService.BuildAttributeNotExistCondition(WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID),
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke($"Failed to update worker-vm-list table for [{VirtualMachineName}]");
                    return false;
                }
            }
            catch (System.Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to update worker-vm-list table for [{VirtualMachineName}]. Error: {ex.Message}. Trace: {ex.StackTrace}");
                return false;
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(_InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), _RequestedVirtualMachineId, _ErrorMessageAction);
            }
            return true;
        }

        private static bool UpdateDBProcessHistory(
            IBDatabaseServiceInterface _DatabaseService,
            WebServiceBaseTimeoutableProcessor _InnerProcessor, 
            WorkerVMListDBEntry _VirtualMachineEntry,
            Action<string> _ErrorMessageAction)
        {
            try
            {
                if (_VirtualMachineEntry.ProcessId == null)
                {
                    _ErrorMessageAction?.Invoke($"Failed to add ProcessHistory record because Process ID is not provided.");
                    return false;
                }

                if (!_DatabaseService.GetItem(
                    ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(),
                    ProcessHistoryDBEntry.KEY_NAME_PROCESS_ID,
                    new BPrimitiveType(_VirtualMachineEntry.ProcessId),
                    ProcessHistoryDBEntry.Properties,
                    out JObject ProcessHistoryResponse,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Process ID could not be retrieved.");
                    return false;
                }

                var ProcessHistoryObject = JsonConvert.DeserializeObject<ProcessHistoryDBEntry>(ProcessHistoryResponse.ToString());

                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(_InnerProcessor, ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(), _VirtualMachineEntry.ProcessId, _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke($"Failed to update process history db for [{_VirtualMachineEntry.ModelName}/{_VirtualMachineEntry.RevisionIndex}] because atomic db operation has been failed.");
                    return false;
                }

                ProcessHistoryObject.ProcessStatus = (int)EProcessStatus.Canceled;
                ProcessHistoryObject.CurrentProcessStage = _VirtualMachineEntry.CurrentProcessStage;

                var NewHistoryRecord = new HistoryRecord();
                NewHistoryRecord.RecordDate = Methods.ToISOString();
                NewHistoryRecord.RecordProcessStage = _VirtualMachineEntry.CurrentProcessStage;
                NewHistoryRecord.ProcessInfo = "Stop process has been called.";
                ProcessHistoryObject.HistoryRecords.Add(NewHistoryRecord);

                if (!_DatabaseService.UpdateItem(
                    ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(),
                    ProcessHistoryDBEntry.KEY_NAME_PROCESS_ID,
                    new BPrimitiveType(_VirtualMachineEntry.ProcessId),
                    JObject.Parse(JsonConvert.SerializeObject(ProcessHistoryObject)),
                    out JObject _, EBReturnItemBehaviour.DoNotReturn,
                    _DatabaseService.BuildAttributeNotExistCondition(ProcessHistoryDBEntry.KEY_NAME_PROCESS_ID),
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke($"Failed to update process-history table for [{_VirtualMachineEntry.ModelName}/{_VirtualMachineEntry.RevisionIndex}]");
                    return false;
                }
            }
            catch (System.Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to add process-history record. Error: {ex.Message}. Trace: {ex.StackTrace}");
                return false;
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(_InnerProcessor, ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(), _VirtualMachineEntry.ProcessId, _ErrorMessageAction);
            }
            return true;
        }

        public static bool InitializeWorkerVMListTable(
            IBDatabaseServiceInterface _DatabaseService,
            Dictionary<string, string> _VirtualMachineDictionary,
            Action<string> _ErrorMessageAction)
        {
            foreach (var VirtualMachine in _VirtualMachineDictionary)
            {
                if (_DatabaseService.GetItem(
                    WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                    WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                    new BPrimitiveType(VirtualMachine.Key),
                    FileConversionDBEntry.Properties,
                    out JObject _CurrentVMEntry
                ) && _CurrentVMEntry == null)
                {
                    try
                    {
                        var _VirtualMachineEntry = new WorkerVMListDBEntry();
                        _VirtualMachineEntry.VMName = VirtualMachine.Value;

                        if (!_DatabaseService.UpdateItem(
                            WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                            WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                            new BPrimitiveType(VirtualMachine.Key),
                            JObject.Parse(JsonConvert.SerializeObject(_VirtualMachineEntry)),
                            out JObject _, EBReturnItemBehaviour.DoNotReturn,
                            _DatabaseService.BuildAttributeNotExistCondition(WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID),
                            _ErrorMessageAction))
                        {
                            _ErrorMessageAction?.Invoke($"InitializeWorkerVMListTable: Failed to update worker-vm-list table for [{VirtualMachine.Value}]");
                            return false;
                        }
                        _ErrorMessageAction?.Invoke($"InitializeWorkerVMListTable: Virtual Machine {VirtualMachine.Value} is saved with {VirtualMachine.Key} id in worker-vm-list table.");
                    }
                    catch (System.Exception ex)
                    {
                        _ErrorMessageAction?.Invoke($"InitializeWorkerVMListTable: Failed to update worker-vm-list table for [{VirtualMachine.Value}]. Error: {ex.Message}. Trace: {ex.StackTrace}");
                        return false;
                    }
                }

                if (_CurrentVMEntry != null)
                {
                    _ErrorMessageAction?.Invoke($"InitializeWorkerVMListTable: Virtual Machine {VirtualMachine.Value} is already in worker-vm-list table with {VirtualMachine.Key} id.");
                }
            }
            
            return true;
        }
    }
}
