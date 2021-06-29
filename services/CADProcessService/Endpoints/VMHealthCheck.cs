/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Net;
using BCloudServiceUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using Newtonsoft.Json.Linq;
using ServiceUtilities;
using Newtonsoft.Json;
using ServiceUtilities.Common;
using BCommonUtilities;
using CADProcessService.Endpoints.Common;
using System.Linq;

namespace CADProcessService.Endpoints
{
    partial class InternalCalls
    {
        internal class VMHealthCheck : InternalWebServiceBaseTimeoutable
        {
            private readonly IBDatabaseServiceInterface DatabaseService;

            private readonly IBVMServiceInterface VirtualMachineService;

            private readonly Dictionary<string, string> VirtualMachineDictionary;

            public VMHealthCheck(IBDatabaseServiceInterface _DatabaseService, IBVMServiceInterface _VirtualMachineService, Dictionary<string, string> _VirtualMachineDictionary, string _InternalCallPrivateKey) : base(_InternalCallPrivateKey)
            {
                DatabaseService = _DatabaseService;
                VirtualMachineService = _VirtualMachineService;
                VirtualMachineDictionary = _VirtualMachineDictionary;
            }

            public override BWebServiceResponse OnRequest_Interruptable(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
            {
                if (_Context.Request.HttpMethod != "GET")
                {
                    _ErrorMessageAction?.Invoke("VMHealthCheck: GET method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                    return BWebResponse.MethodNotAllowed("GET method is accepted. But received request method: " + _Context.Request.HttpMethod);
                }

                Check_VirtualMachines(_ErrorMessageAction);

                return BWebResponse.StatusOK("OK.");
            }

            private void Check_VirtualMachines(Action<string> _ErrorMessageAction)
            {
                if (!DatabaseService.ScanTable(WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), out List<JObject> WorkerVMJList, _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Scan-table operation has failed.");
                    return;
                }

                if (WorkerVMJList.Count == 0)
                {
                    return;
                }

                foreach (var CurrentWorkerVMJObject in WorkerVMJList)
                {
                    var CurrentWorkerVM = JsonConvert.DeserializeObject<WorkerVMListDBEntry>(CurrentWorkerVMJObject.ToString());
                    var _RequestedVirtualMachineId = VirtualMachineDictionary.FirstOrDefault(x => x.Value.Equals(CurrentWorkerVM.VMName)).Key;

                    if (CurrentWorkerVM.VMStatus == (int)EVMStatus.Available || CurrentWorkerVM.VMStatus == (int)EVMStatus.NotResponding)
                    {
                        if (!CommonMethods.StopVirtualMachine(
                            VirtualMachineService,
                            DatabaseService,
                            InnerProcessor,
                            CurrentWorkerVM,
                            _RequestedVirtualMachineId,
                            true,
                            _ErrorMessageAction,
                            out BWebServiceResponse _FailureResponse))
                        {
                            if (_FailureResponse.ResponseContent.Type == EBStringOrStreamEnum.String)
                            {
                                _ErrorMessageAction?.Invoke(_FailureResponse.ResponseContent.String);
                            }
                            return;
                        }
                    }
                }
            }
        }
    }
}
