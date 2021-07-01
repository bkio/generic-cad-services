/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.IO;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using CADProcessService.K8S;
using ServiceUtilities.All;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using CADProcessService.Endpoints.Utilities;
using ServiceUtilities.Common;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using ServiceUtilities;
using System.Threading;

namespace CADProcessService.Endpoints
{
    internal class StartProcessRequest : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly IBVMServiceInterface VirtualMachineService;
        private readonly Dictionary<string, string> VirtualMachineDictionary;
        private readonly string CadProcessServiceUrl;
        private readonly Dictionary<int, OptimizationPresetEntry> Presets = new Dictionary<int, OptimizationPresetEntry>();

        public StartProcessRequest(IBDatabaseServiceInterface _DatabaseService, IBVMServiceInterface _VirtualMachineService, Dictionary<string, string> _VirtualMachineDictionary, string _CadProcessServiceUrl) : base()
        {
            CreateDefaultPresets();
            DatabaseService = _DatabaseService;
            VirtualMachineService = _VirtualMachineService;
            VirtualMachineDictionary = _VirtualMachineDictionary;
            CadProcessServiceUrl = _CadProcessServiceUrl;
        }

        public override BWebServiceResponse OnRequest_Interruptable_DeliveryEnsurerUser(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            GetTracingService()?.On_FromServiceToService_Received(_Context, _ErrorMessageAction);

            var Result = OnRequest_Internal(_Context, _ErrorMessageAction);

            GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse OnRequest_Internal(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            //TestVMStart(_ErrorMessageAction);
            if (_Context.Request.HttpMethod != "POST")
            {
                _ErrorMessageAction?.Invoke("StartProcessRequest: POST methods is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("POST methods is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            var NewFileConversionDBEntry = new FileConversionDBEntry()
            {
                ConversionStatus = (int)EInternalProcessStage.Queued
            };

            using (var InputStream = _Context.Request.InputStream)
            {
                var NewObjectJson = new JObject();

                using (var ResponseReader = new StreamReader(InputStream))
                {
                    try
                    {
                        var ParsedBody = JObject.Parse(ResponseReader.ReadToEnd());

                        if (!ParsedBody.ContainsKey("bucketName") ||
                            !ParsedBody.ContainsKey("fileRelativeUrl"))
                        {
                            _ErrorMessageAction?.Invoke("StartProcessRequest: Request body must contain all necessary fields.");
                            return BWebResponse.BadRequest("StartProcessRequest: Request body must contain all necessary fields.");
                        }

                        var BucketNameToken = ParsedBody["bucketName"];
                        var FileRelativeUrlToken = ParsedBody["fileRelativeUrl"];
                        if (BucketNameToken.Type != JTokenType.String ||
                            FileRelativeUrlToken.Type != JTokenType.String)
                        {
                            _ErrorMessageAction?.Invoke("StartProcessRequest: Request body contains invalid fields.");
                            return BWebResponse.BadRequest("Request body contains invalid fields.");
                        }
                        string BucketName = (string)BucketNameToken;
                        string RelativeFileName = (string)FileRelativeUrlToken;

                        if (BucketName == null || RelativeFileName == null)
                        {
                            _ErrorMessageAction?.Invoke("StartProcessRequest: No bucketName or fileRelativeUrl.");
                            return BWebResponse.InternalError("No bucketName or fileRelativeUrl.");
                        }

                        NewFileConversionDBEntry.BucketName = BucketName;
                        NewFileConversionDBEntry.QueuedTime = DateTime.UtcNow.ToString();

                        string ModelId = null;
                        if (ParsedBody.ContainsKey("modelId"))
                        {
                            ModelId = (string)ParsedBody["modelId"];
                        }
                        if (ParsedBody.ContainsKey("modelName"))
                        {
                            NewFileConversionDBEntry.ModelName = (string)ParsedBody["modelName"];
                        }
                        if (ParsedBody.ContainsKey("modelRevision"))
                        {
                            NewFileConversionDBEntry.ModelRevision = (int)ParsedBody["modelRevision"];
                        }
                        if (ParsedBody.ContainsKey("zipMainAssemblyFileNameIfAny"))
                        {
                            NewFileConversionDBEntry.ZipMainAssemblyFileNameIfAny = (string)ParsedBody["zipMainAssemblyFileNameIfAny"];
                        }
                        if (ParsedBody.ContainsKey("processStep"))
                        {
                            NewFileConversionDBEntry.ConversionStage = (int)ParsedBody["processStep"];
                        }
                        if (ParsedBody.ContainsKey("globalScale"))
                        {
                            NewFileConversionDBEntry.GlobalScale = (float)ParsedBody["globalScale"];
                        }
                        if (ParsedBody.ContainsKey("globalXOffset"))
                        {
                            NewFileConversionDBEntry.GlobalXOffset = (float)ParsedBody["globalXOffset"];
                        }
                        if (ParsedBody.ContainsKey("globalYOffset"))
                        {
                            NewFileConversionDBEntry.GlobalYOffset = (float)ParsedBody["globalYOffset"];
                        }
                        if (ParsedBody.ContainsKey("globalZOffset"))
                        {
                            NewFileConversionDBEntry.GlobalZOffset = (float)ParsedBody["globalZOffset"];
                        }
                        if (ParsedBody.ContainsKey("globalXRotation"))
                        {
                            NewFileConversionDBEntry.GlobalXRotation = (float)ParsedBody["globalXRotation"];
                        }
                        if (ParsedBody.ContainsKey("globalYRotation"))
                        {
                            NewFileConversionDBEntry.GlobalYRotation = (float)ParsedBody["globalYRotation"];
                        }
                        if (ParsedBody.ContainsKey("globalZRotation"))
                        {
                            NewFileConversionDBEntry.GlobalZRotation = (float)ParsedBody["globalZRotation"];
                        }
                        if (ParsedBody.ContainsKey("levelThresholds"))
                        {
                            NewFileConversionDBEntry.LevelThresholds = (string)ParsedBody["levelThresholds"];
                        }
                        if (ParsedBody.ContainsKey("lodParameters"))
                        {
                            NewFileConversionDBEntry.LodParameters = (string)ParsedBody["lodParameters"];
                        }
                        if (ParsedBody.ContainsKey("cullingThresholds"))
                        {
                            NewFileConversionDBEntry.CullingThresholds = (string)ParsedBody["cullingThresholds"];
                        }
                        if (ParsedBody.ContainsKey("filters"))
                        {
                            NewFileConversionDBEntry.FilterSettings = (string)ParsedBody["filters"];
                        }
                        if (ParsedBody.ContainsKey("deleteDuplicates"))
                        {
                            NewFileConversionDBEntry.DeleteDuplicates = (string)ParsedBody["deleteDuplicates"];
                        }
                        if (ParsedBody.ContainsKey("customPythonScript"))
                        {
                            NewFileConversionDBEntry.CustomPythonScript = (string)ParsedBody["customPythonScript"];
                        }
                        if (ParsedBody.ContainsKey("mergeFinalLevel"))
                        {
                            NewFileConversionDBEntry.MergeFinalLevel = (string)ParsedBody["mergeFinalLevel"];
                        }
                        if (ParsedBody.ContainsKey("optimizationPreset"))
                        {
                            //If Preset is used then override optimization parameters
                            int _PresetId = (int)ParsedBody["optimizationPreset"];
                            OptimizationPresetEntry Preset = Presets[_PresetId];
                            NewFileConversionDBEntry.LodParameters = Preset.LodParameters;
                            NewFileConversionDBEntry.CullingThresholds = Preset.CullingThresholds;
                            NewFileConversionDBEntry.LevelThresholds = Preset.DistanceThresholds;
                        }

                        if (!UpdateFileConversionEntry(ModelId, NewFileConversionDBEntry, _ErrorMessageAction, out BWebServiceResponse FailureResponse))
                        {
                            return FailureResponse;
                        }

                        if (!UpdateWorkerVMEntry(ModelId, NewFileConversionDBEntry, _ErrorMessageAction, out FailureResponse))
                        {
                            return FailureResponse;
                        }

                        //if (StartBatchProcess(RelativeFileName, NewFileConversionDBEntry, _ErrorMessageAction, out BWebServiceResponse SuccessResponse, out FailureResponse))
                        //{
                        //    return SuccessResponse;
                        //}
                        //else
                        //{
                        //    return FailureResponse;
                        //}
                    }
                    catch (Exception e)
                    {
                        _ErrorMessageAction?.Invoke("Read request body stage has failed. Exception: " + e.Message + ", Trace: " + e.StackTrace);
                        return BWebResponse.BadRequest("Malformed request body. Request must be a valid json form.");
                    }
                }
            }

            return BWebResponse.StatusAccepted("Request has been accepted; process is now being started.");
        }

        private bool UpdateFileConversionEntry(string ModelId, FileConversionDBEntry _FileConversionEntry, Action<string> _ErrorMessageAction, out BWebServiceResponse _FailureResponse)
        {
            _FailureResponse = BWebResponse.InternalError("");

            try
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), ModelId, _ErrorMessageAction))
                {
                    _FailureResponse = BWebResponse.InternalError($"StartProcessRequest:UpdateFileConversionEntry-> Failed to get access to database record");
                    return false;
                }

                //If a process was completed (success or failure) then allow reprocessing
                //Only stop if a process is currently busy processing or already queued
                if (DatabaseService.GetItem(
                    FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                    FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                    new BPrimitiveType(ModelId),
                    FileConversionDBEntry.Properties,
                    out JObject ConversionObject
                    ))
                {
                    if (ConversionObject != null && ConversionObject.ContainsKey("conversionStatus"))
                    {
                        EInternalProcessStage ExistingStatus = (EInternalProcessStage)(int)ConversionObject["conversionStatus"];

                        if (ExistingStatus == EInternalProcessStage.Processing)
                        {
                            _FailureResponse = BWebResponse.Conflict("StartProcessRequest:UpdateFileConversionEntry-> File is already being processed/queued.");
                            return false;
                        }
                    }
                }

                if (!DatabaseService.UpdateItem(
                    FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                    FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                    new BPrimitiveType(ModelId),
                    JObject.Parse(JsonConvert.SerializeObject(_FileConversionEntry)),
                    out JObject _, EBReturnItemBehaviour.DoNotReturn,
                    null,
                    _ErrorMessageAction))
                {
                    _FailureResponse = BWebResponse.Conflict("StartProcessRequest:UpdateFileConversionEntry-> DBSERVICE_FILE_CONVERSIONS_TABLE File UpdateItem database error.");
                    return false;
                }
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"StartProcessRequest:UpdateFileConversionEntry-> An error occurred. Error Message: {ex.Message}\nStackTrace: {ex.StackTrace}");
                _FailureResponse = BWebResponse.InternalError($"StartProcessRequest:UpdateFileConversionEntry->An error occurred.");
                return false;
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), ModelId, _ErrorMessageAction);
            }
            return true;
        }

        private bool UpdateWorkerVMEntry(string ModelId, FileConversionDBEntry _FileConversionEntry, Action<string> _ErrorMessageAction, out BWebServiceResponse _FailureResponse)
        {
            _FailureResponse = BWebResponse.InternalError("");

            try
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), "VMLOCK", _ErrorMessageAction))
                {
                    _FailureResponse = BWebResponse.InternalError($"StartProcessRequest:UpdateWorkerVMEntry->DBSERVICE_WORKERS_VM_LIST_TABLE: Failed to get access to database lock");
                    return false;
                }

                WorkerVMListDBEntry VmEntry = GetAvailableVm(out string _VMID, out string _VMName, _ErrorMessageAction);
                if (VmEntry != null)
                {
                    //for reverting back to original vm entry when the vm fails to start.
                    var PreviousVMEntry = JObject.Parse(JsonConvert.SerializeObject(VmEntry));

                    VmEntry.CurrentProcessStage = _FileConversionEntry.ConversionStage;
                    VmEntry.LastKnownProcessStatus = 0;
                    VmEntry.LastKnownProcessStatusInfo = "Init";
                    VmEntry.ProcessStartDate = Methods.ToISOString();
                    VmEntry.ProcessEndDate = "";
                    VmEntry.VMStatus = (int)EVMStatus.Busy;
                    VmEntry.ModelName = _FileConversionEntry.ModelName;
                    VmEntry.ProcessId = ModelId;
                    VmEntry.RevisionIndex = _FileConversionEntry.ModelRevision;

                    if (!DatabaseService.UpdateItem(
                        WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                        WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                        new BPrimitiveType(_VMID),
                        JObject.Parse(JsonConvert.SerializeObject(VmEntry)),
                        out JObject _, EBReturnItemBehaviour.DoNotReturn,
                        null,
                        _ErrorMessageAction))
                    {
                        _ErrorMessageAction?.Invoke($"StartProcessRequest:UpdateWorkerVMEntry->Update WorkerVMListDBEntry record before start virtual machine.");
                    }

                    StartVirtualMachine(_VMID, _VMName, VmEntry, () =>
                    {
                        _ErrorMessageAction?.Invoke($"StartProcessRequest:UpdateWorkerVMEntry->Virtual Machine starting operation has been failed.");
                        //if starting vm fails, revert back to original vm entry
                        if (!DatabaseService.UpdateItem(
                            WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                            WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                            new BPrimitiveType(_VMID),
                            PreviousVMEntry,
                            out JObject _, EBReturnItemBehaviour.DoNotReturn,
                            null,
                            _ErrorMessageAction))
                        {
                            _ErrorMessageAction?.Invoke($"StartProcessRequest:UpdateWorkerVMEntry-> VM entry record couldn't revert back after starting virtual machine failed.");
                        }
                    }, _ErrorMessageAction);
                }
                else
                {
                    _ErrorMessageAction?.Invoke($"StartProcessRequest:UpdateWorkerVMEntry->There is no available VM for now.");
                }
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"StartProcessRequest:UpdateWorkerVMEntry-> An error occurred. Error Message: {ex.Message}\nStackTrace: {ex.StackTrace}");
                _FailureResponse = BWebResponse.InternalError($"StartProcessRequest:UpdateWorkerVMEntry->An error occurred.");
                return false;
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), "VMLOCK", _ErrorMessageAction);
            }

            return true;
        }

        public WorkerVMListDBEntry GetAvailableVm(out string _Id, out string _VmName, Action<string> _ErrorMessageAction)
        {
            //Fetch VM entries
            //Find VM that is available and return name.
            foreach (var vm in VirtualMachineDictionary)
            {
                if (DatabaseService.GetItem(
                    WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                    WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                    new BPrimitiveType(vm.Key),
                    FileConversionDBEntry.Properties,
                    out JObject VMEntry
                ))
                {
                    WorkerVMListDBEntry CurrentEntry = VMEntry.ToObject<WorkerVMListDBEntry>();

                    if ((EVMStatus)CurrentEntry.VMStatus == EVMStatus.Available || (EVMStatus)CurrentEntry.VMStatus == EVMStatus.Stopped)
                    {
                        _Id = vm.Key;
                        _VmName = vm.Value;
                        return CurrentEntry;
                    }
                }
            }
            _Id = null;
            _VmName = null;
            return null;
        }

        private void RunCommandOnVirtualMachine(WorkerVMListDBEntry _VirtualMachineEntry, string _VirtualMachineName, string _VirtualMachineId, Action<string> _ErrorMessageAction)
        {
            VirtualMachineService.RunCommand(new string[] { _VirtualMachineName }, EBVMOSType.Windows,
                new string[] {
                    $"Start-Process \"cmd.exe\" \"/c C:\\Applet\\LaunchUpdater.bat {_VirtualMachineId} {CadProcessServiceUrl}\"",
                },
                () =>
                {
                    _ErrorMessageAction?.Invoke($"Command has been executed. VM Name: [{_VirtualMachineName}] - Model Name: [{_VirtualMachineEntry.ModelName}] - Revision Index: [{_VirtualMachineEntry.RevisionIndex}]");
                },
                () =>
                {
                    _ErrorMessageAction?.Invoke($"Command execution has been failed. VM Name: [{_VirtualMachineName}] - Model Name: [{_VirtualMachineEntry.ModelName}] - Revision Index: [{_VirtualMachineEntry.RevisionIndex}]");
                }
            );
        }

        private void StartVirtualMachine(string _VirtualMachineId, string _VirtualMachineName, WorkerVMListDBEntry _VirtualMachineEntry, System.Action VMStartFailureAction, Action<string> _ErrorMessageAction)
        {
            if (_VirtualMachineEntry != null)
            {
                if (VirtualMachineService.GetInstanceStatus(_VirtualMachineName, out EBVMInstanceStatus VMStatus, _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke($"VM Status has been received. VM Name: [{_VirtualMachineName}] - Model Name: [{_VirtualMachineEntry.ModelName}] - Revision Index: [{_VirtualMachineEntry.RevisionIndex}] - Status: [{VMStatus.ToString()}]");

                    if (VMStatus == EBVMInstanceStatus.Running)
                    {
                        RunCommandOnVirtualMachine(_VirtualMachineEntry, _VirtualMachineName, _VirtualMachineId, _ErrorMessageAction);
                    }
                    else
                    {
                        VirtualMachineService.StartInstances(new string[] { _VirtualMachineName },
                        () =>
                        {
                            _ErrorMessageAction?.Invoke($"Virtual Machine has been started. VM Name: [{_VirtualMachineName}] - Model Name: [{_VirtualMachineEntry.ModelName}] - Revision Index: [{_VirtualMachineEntry.RevisionIndex}]");
                            RunCommandOnVirtualMachine(_VirtualMachineEntry, _VirtualMachineName, _VirtualMachineId, _ErrorMessageAction);
                        },
                        () =>
                        {
                            _ErrorMessageAction?.Invoke($"Virtual Machine starting has been failed. VM Name: [{_VirtualMachineName}] - Model Name: [{_VirtualMachineEntry.ModelName}] - Revision Index: [{_VirtualMachineEntry.RevisionIndex}]");
                            VMStartFailureAction();
                        },
                            _ErrorMessageAction
                        );
                    }
                }
                else
                {
                    _ErrorMessageAction?.Invoke($"VM Status receiving has been failed. VM Name: [{_VirtualMachineName}] - Model Name: [{_VirtualMachineEntry.ModelName}] - Revision Index: [{_VirtualMachineEntry.RevisionIndex}]");
                }
            }
            else
            {
                _ErrorMessageAction?.Invoke($"No available virtual machine was found. VM Name: [{_VirtualMachineName}] - Model Name: [{_VirtualMachineEntry.ModelName}] - Revision Index: [{_VirtualMachineEntry.RevisionIndex}]");
                VMStartFailureAction();
            }
        }

        private void TestVMStart(Action<string> _ErrorMessageAction)
        {
            ManualResetEvent Wait = new ManualResetEvent(false);
            string _VirtualMachineName = "cip-vm-f027aa53-2";

            
                if (VirtualMachineService.GetInstanceStatus(_VirtualMachineName, out EBVMInstanceStatus VMStatus, _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke($"VM Status has been received. Name: [{_VirtualMachineName}] - Status: [{VMStatus.ToString()}]");

                    if (VMStatus == EBVMInstanceStatus.Running)
                    {
                        _ErrorMessageAction?.Invoke($"Virtual Machine has been started. Name: [{_VirtualMachineName}]");
                        VirtualMachineService.RunCommand(new string[] { _VirtualMachineName }, EBVMOSType.Windows,
                        new string[] {
                                    $"Start-Process \"cmd.exe\" \"/c C:\\Applet\\LaunchUpdater.bat 03598d72-251c-4a61-90a1-8493c46e7419 https://api-cip-dev.kognitwin.com/f027aa53/\"",
                                    //$"Start-Process \"cmd.exe\" \"/c C:\\Applet\\Test.bat {_VirtualMachineName} {CadProcessServiceUrl}\"",
                                },
                        () =>
                        {
                            _ErrorMessageAction?.Invoke($"Command has been executed. Name: [{_VirtualMachineName}]");
                            Wait.Set();
                        },
                        () =>
                        {
                            _ErrorMessageAction?.Invoke($"Command execution has been failed. Name: [{_VirtualMachineName}]");
                            Wait.Set();
                        }
                    );
                }
                    else
                    {
                        VirtualMachineService.StartInstances(new string[] { _VirtualMachineName },
                        () =>
                        {
                            _ErrorMessageAction?.Invoke($"Virtual Machine has been started. Name: [{_VirtualMachineName}]");
                            VirtualMachineService.RunCommand(new string[] { _VirtualMachineName }, EBVMOSType.Windows,
                                new string[] {
                                    $"Start-Process \"cmd.exe\" \"/c C:\\Applet\\LaunchUpdater.bat 03598d72-251c-4a61-90a1-8493c46e7419 {CadProcessServiceUrl}\"",
                                    //$"Start-Process \"cmd.exe\" \"/c C:\\Applet\\Test.bat {_VirtualMachineName} {CadProcessServiceUrl}\"",
                                },
                                () =>
                                {
                                    _ErrorMessageAction?.Invoke($"Command has been executed. Name: [{_VirtualMachineName}]");
                                    Wait.Set();
                                },
                                () =>
                                {
                                    _ErrorMessageAction?.Invoke($"Command execution has been failed. Name: [{_VirtualMachineName}]");
                                    Wait.Set();
                                }
                            );
                        },
                        () =>
                        {
                            _ErrorMessageAction?.Invoke($"Virtual Machine starting has been failed. Name: [{_VirtualMachineName}]");
                            
                        },
                            _ErrorMessageAction
                        );
                    }
                }
                else
                {
                    _ErrorMessageAction?.Invoke($"VM Status receiving has been failed. Name: [{_VirtualMachineName}]");
                }
            
            Wait.WaitOne();
        }

        private void CreateDefaultPresets()
        {
            Presets.Add(0, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 50, 200, 500, 1000, 4000, 10000]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 1.0f, 4.0f, 8.0f, 15.0f, 30.0f, 60.0f, 90.0f }),
                LodParameters = "[[14.0, -1, 15, 100.0], [60.0, 50.0, -1, 80.0], [120.0, 100.0, -1, 60.0], [200.0, 200.0, -1, 50.0], [400.0, 400.0, -1, 30.0], [800.0, 800.0, -1, 20.0], [1000.0, 1000.0, 50.0, 10.0]]"
            });

            Presets.Add(1, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 50, 200, 500, 1000, 4000, 10000]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 1.0f, 4.0f, 8.0f, 15.0f, 30.0f, 60.0f, 90.0f }),
                LodParameters = "[[14.0, -1, 15.0, 100.0], [60.0, 50.0, -1, 80.0], [120.0, 100.0, -1, 60.0], [200.0, 200.0, -1, 50.0], [400.0, 400.0, -1, 30.0], [800.0, 800.0, -1, 20.0], [1000.0, 1000.0, 50.0, 10.0]]"
            });

            Presets.Add(2, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 0, 1000, 3000, 6000, 9500]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 2.0f, 6.0f, 20.0f, 100.0f, 150.0f, 300.0f }),
                LodParameters = "[[6.0, -1, 15.0, 100.0], [12.0, -1, 30.0, 50.0], [20.0, -1, 50.0, 30.0], [30.0, -1, 60.0, 16.0], [60.0, -1, 60.0, 10.0], [100.0, -1, 60.0, 5.0]]"
            });

            Presets.Add(3, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 0, 1000, 3000, 6000, 9500]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 2.0f, 6.0f, 20.0f, 100.0f, 150.0f, 300.0f }),
                LodParameters = "[[6.0, -1, 30.0, 100.0], [12.0, -1, 40.0, 50.0], [20.0, -1, 50.0, 30.0], [30.0, -1, 60.0, 16.0], [60.0, -1, 60.0, 10.0], [100.0, -1, 60.0, 5.0]]"
            });

            Presets.Add(4, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 0, 1000, 3000, 6000, 8000]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 2.0f, 6.0f, 20.0f, 100.0f, 150.0f, 300.0f }),
                LodParameters = "[[6.0, -1, 15.0, 100.0], [12.0, -1, 30.0, 60.0], [20.0, -1, 40.0, 40.0], [30.0, -1, 60.0, 20.0], [60.0, -1, 60.0, 10.0], [100.0, -1, 60.0, 5.0]]"
            });

            Presets.Add(5, new OptimizationPresetEntry
            {
                CullingThresholds = "[0, 0, 1000, 3000, 6000, 9500]",
                DistanceThresholds = JsonConvert.SerializeObject(new float[] { 2.0f, 6.0f, 20.0f, 100.0f, 150.0f, 300.0f }),
                LodParameters = "[[6.0, -1, 15.0, 100.0], [12.0, -1, 30.0, 60.0], [20.0, -1, 40.0, 40.0], [30.0, -1, 60.0, 20.0], [60.0, -1, 60.0, 10.0], [100.0, -1, 60.0, 5.0]]"
            });
        }

        private bool StartBatchProcess(string RelativeFileName, FileConversionDBEntry _FileConversionEntry, Action<string> _ErrorMessageAction, out BWebServiceResponse _SuccessResponse, out BWebServiceResponse _FailureResponse)
        {
            _FailureResponse = BWebResponse.InternalError("");
            _SuccessResponse = BWebResponse.StatusAccepted("");

            var DeploymentBranchName = Resources_DeploymentManager.Get().GetDeploymentBranchName();
            var NewConversionID_FromRelativeUrl_UrlEncoded = WebUtility.UrlEncode($"{DeploymentBranchName}/{_FileConversionEntry.ModelName}/{_FileConversionEntry.ModelRevision}");

            try
            {
                if (BatchProcessingCreationService.Instance.StartBatchProcess(_FileConversionEntry.BucketName, RelativeFileName, _FileConversionEntry.ZipMainAssemblyFileNameIfAny, out string _PodName, _ErrorMessageAction))
                {
                    //Code for initial method of starting optimizer after pixyz completes
                    //return BWebResponse.StatusAccepted("Request has been accepted; process is now being started.");
                    if (BatchProcessingCreationService.Instance.StartFileOptimizer(_FileConversionEntry.BucketName, RelativeFileName, _ErrorMessageAction))
                    {
                        _SuccessResponse = BWebResponse.StatusAccepted("Request has been accepted; process is now being started.");
                        _ErrorMessageAction?.Invoke($"StartBatchProcess-> Request has been accepted; process is now being started.");
                        return true;
                    }
                    else
                    {
                        _FileConversionEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;

                        if (!DatabaseService.UpdateItem(
                            FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                            FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                            new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
                            JObject.Parse(JsonConvert.SerializeObject(_FileConversionEntry)),
                            out JObject _, EBReturnItemBehaviour.DoNotReturn,
                            null,
                            _ErrorMessageAction))
                        {
                            _FailureResponse = BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
                            _ErrorMessageAction?.Invoke($"StartBatchProcess-> Failed to start the batch process and experienced a Database error");
                            return false;
                        }

                        //Try kill pixyz pod that we have succeeded in creating
                        if (!BatchProcessingCreationService.Instance.TryKillPod(_PodName, "cip-batch"))
                        {
                            _FailureResponse = BWebResponse.InternalError("Failed to start the unreal optimizer and failed to kill pixyz pod");
                            _ErrorMessageAction?.Invoke($"StartBatchProcess-> Failed to start the unreal optimizer and failed to kill pixyz pod");
                            return false;
                        }

                        _FailureResponse = BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
                        _ErrorMessageAction?.Invoke($"StartBatchProcess-> Failed to start the batch process and experienced a Database error");
                        return false;
                    }
                }
                else
                {
                    _FileConversionEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;

                    if (!DatabaseService.UpdateItem(
                        FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                        FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                        new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
                        JObject.Parse(JsonConvert.SerializeObject(_FileConversionEntry)),
                        out JObject _, EBReturnItemBehaviour.DoNotReturn,
                        null,
                        _ErrorMessageAction))
                    {
                        _FailureResponse = BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
                        _ErrorMessageAction?.Invoke($"StartBatchProcess-> Failed to start the batch process and experienced a Database error");
                        return false;
                    }

                    _FailureResponse = BWebResponse.InternalError("Failed to start the batch process");
                    _ErrorMessageAction?.Invoke($"StartBatchProcess-> Failed to start the batch process");
                    return false;
                }
            }
            catch (Exception ex)
            {
                _ErrorMessageAction?.Invoke($"StartBatchProcess-> Error Message: {ex.Message}\n StackTrace: {ex.StackTrace}");

                _FileConversionEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;

                if (!DatabaseService.UpdateItem(
                    FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                    FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                    new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
                    JObject.Parse(JsonConvert.SerializeObject(_FileConversionEntry)),
                    out JObject _, EBReturnItemBehaviour.DoNotReturn,
                    null,
                    _ErrorMessageAction))
                {
                    _FailureResponse = BWebResponse.InternalError("Failed to start the batch process and experienced a Database error");
                    _ErrorMessageAction?.Invoke($"StartBatchProcess-> Failed to start the batch process and experienced a Database error");
                    return false;
                }

                _FailureResponse = BWebResponse.InternalError("Failed to start the batch process");
                _ErrorMessageAction?.Invoke($"StartBatchProcess-> Failed to start the batch process");
                return false;
            }
        }
    }
}