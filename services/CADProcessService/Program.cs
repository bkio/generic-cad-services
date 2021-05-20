/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Threading;
using BCloudServiceUtilities;
using BServiceUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints;
using CADProcessService.K8S;
using ServiceUtilities;
using ServiceUtilities.Process.Procedure;
using ServiceUtilities.All;
using k8s.Models;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace CADProcessService
{
    class Program
    {
        static void Main()
        {
            Console.WriteLine("Initializing the service...");

//#if (Debug || DEBUG)
//            if (!ServicesDebugOnlyUtilities.CalledFromMain()) return;
//#endif

            // In case of a cloud component dependency or environment variable is added/removed;

            /*
            * Common initialization step
            */
            if (!BServiceInitializer.Initialize(out BServiceInitializer ServInit,
                new string[][]
                {
                    new string[] { "AZ_SUBSCRIPTION_ID" },
                    new string[] { "AZ_TENANT_ID" },
                    new string[] { "AZ_CLIENT_ID" },
                    new string[] { "AZ_CLIENT_SECRET" },

                    new string[] { "AZ_RESOURCE_GROUP_NAME" },
                    new string[] { "AZ_RESOURCE_GROUP_LOCATION" },

                    new string[] { "AZ_STORAGE_SERVICE_URL" },
                    new string[] { "AZ_STORAGE_ACCOUNT_NAME" },
                    new string[] { "AZ_STORAGE_ACCOUNT_ACCESS_KEY" },

                    new string[] { "AZ_SERVICEBUS_NAMESPACE_ID" },
                    new string[] { "AZ_SERVICEBUS_NAMESPACE_CONNECTION_STRING" },

                    //new string[] { "MONGODB_CONNECTION_STRING" },
                    new string[] { "MONGODB_CLIENT_CONFIG" },
                    new string[] { "MONGODB_PASSWORD" },
                    new string[] { "MONGODB_DATABASE" },

                    new string[] { "DEPLOYMENT_BRANCH_NAME" },
                    new string[] { "DEPLOYMENT_BUILD_NUMBER" },
                    new string[] { "INTERNAL_CALL_PRIVATE_KEY" },

                    new string[] { "REDIS_ENDPOINT" },
                    new string[] { "REDIS_PORT" },
                    new string[] { "REDIS_PASSWORD" },
                    new string[] { "REDIS_SSL_ENABLED" },

                    new string[] { "CAD_FILE_STORAGE_BUCKET" },

                    new string[] { "CAD_PROCESS_SERVICE_NAME" },
                    new string[] { "CAD_PROCESS_POD_NAME" },
                    new string[] { "CAD_PROCESS_PORT" },

                    new string[] { "CLUSTER_MASTER_ENDPOINT" },
                    new string[] { "CLUSTER_CLIENT_KEY" },
                    new string[] { "CLUSTER_CLIENT_CERTIFICATE" },

                    new string[] { "CAD_READER_IMAGE" },
                    new string[] { "FILE_WORKER_IMAGE" },
                    new string[] { "FILE_OPTIMIZER_IMAGE" },
                    new string[] { "FILE_OPTIMIZER_ENVIRONMENT_VARIABLES" },

                    new string[] { "VM_ADMIN_USERNAME" },
                    new string[] { "VM_ADMIN_PASSWORD" },
                    new string[] { "VM_UUID_NAME_LIST" }
                }))
                return;

            bool bInitSuccess = true;
            bInitSuccess &= ServInit.WithDatabaseService();
            bInitSuccess &= ServInit.WithFileService();
            //bInitSuccess &= ServInit.WithTracingService();
            bInitSuccess &= ServInit.WithPubSubService();
            bInitSuccess &= ServInit.WithMemoryService();
            bInitSuccess &= ServInit.WithVMService();
            if (!bInitSuccess) return;

            Resources_DeploymentManager.Get().SetDeploymentBranchNameAndBuildNumber(ServInit.RequiredEnvironmentVariables["DEPLOYMENT_BRANCH_NAME"], ServInit.RequiredEnvironmentVariables["DEPLOYMENT_BUILD_NUMBER"]);

            Manager_PubSubService.Get().Setup(ServInit.PubSubService);

            try
            {
                //Initialize kubernetes client credentials and init pixyz processing classes
                KubernetesClientManager.SetDefaultCredentials(ServInit.RequiredEnvironmentVariables["CLUSTER_MASTER_ENDPOINT"], ServInit.RequiredEnvironmentVariables["CLUSTER_CLIENT_KEY"], ServInit.RequiredEnvironmentVariables["CLUSTER_CLIENT_CERTIFICATE"]);

                K8sObjectManager.SetImageNames(ServInit.RequiredEnvironmentVariables["FILE_WORKER_IMAGE"], ServInit.RequiredEnvironmentVariables["CAD_READER_IMAGE"], ServInit.RequiredEnvironmentVariables["FILE_OPTIMIZER_IMAGE"]);

                if (!BatchProcessingStateService.Initialize(ServInit.MemoryService,
                    (string Message) =>
                    {
                        ServInit.LoggingService.WriteLogs(BLoggingServiceMessageUtility.Single(EBLoggingServiceLogType.Info, Message), ServInit.ProgramID, "WebService");
                    }))
                {
                    return;
                }

                // Pass CadProcessService environment variables to FileOptimizer like Google, Azure, AWS Credentials etc.
                var FileOptimizerEnvironmentVariables = new Dictionary<string, string>();
                try
                {
                    var FileOptimizerEnvVarsArray = JArray.Parse(ServInit.RequiredEnvironmentVariables["FILE_OPTIMIZER_ENVIRONMENT_VARIABLES"]);
                    foreach (var item in FileOptimizerEnvVarsArray)
                    {
                        if (item.Type == JTokenType.String)
                        {
                            var key = (string)item;
                            if (ServInit.RequiredEnvironmentVariables.ContainsKey(key))
                            {
                                FileOptimizerEnvironmentVariables.Add(key, ServInit.RequiredEnvironmentVariables[key]);
                            }
                        }
                    }
                }
                catch (Exception) { }

                BatchProcessingCreationService.Initialize(
                    ServInit.DatabaseService,
                    ServInit.FileService,
                    ServInit.RequiredEnvironmentVariables["DEPLOYMENT_BRANCH_NAME"],
                    ServInit.RequiredEnvironmentVariables["DEPLOYMENT_BUILD_NUMBER"],
                    ServInit.RequiredEnvironmentVariables["CAD_PROCESS_SERVICE_NAME"],
                    FileOptimizerEnvironmentVariables,
                    () =>
                    {
                        ServInit.LoggingService.WriteLogs(BLoggingServiceMessageUtility.Single(EBLoggingServiceLogType.Info, "Failed to initialize batch process. Exiting..."), ServInit.ProgramID, "WebService");
                        Environment.Exit(1);
                    },
                    (string Message) =>
                    {
                        ServInit.LoggingService.WriteLogs(BLoggingServiceMessageUtility.Single(EBLoggingServiceLogType.Info, Message), ServInit.ProgramID, "WebService");
                    });

            }
            catch (Exception ex)
            {
                ServInit.LoggingService.WriteLogs(BLoggingServiceMessageUtility.Single(EBLoggingServiceLogType.Info, $"{ex.Message}\n{ ex.StackTrace}"), ServInit.ProgramID, "WebService");

                return;
            }

            Dictionary<string, string> VirtualMachineDictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(ServInit.RequiredEnvironmentVariables["VM_UUID_NAME_LIST"]);

            var CadFileStorageBucketName = ServInit.RequiredEnvironmentVariables["CAD_FILE_STORAGE_BUCKET"];

            var RootPath = "/";
            if (ServInit.RequiredEnvironmentVariables["DEPLOYMENT_BRANCH_NAME"] != "master" && ServInit.RequiredEnvironmentVariables["DEPLOYMENT_BRANCH_NAME"] != "development")
            {
                RootPath = "/" + ServInit.RequiredEnvironmentVariables["DEPLOYMENT_BRANCH_NAME"] + "/";
            }

            var InitializerThread = new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;

                Controller_AtomicDBOperation.Get().StartTimeoutCheckOperation(WebServiceBaseTimeoutableProcessor.OnTimeoutNotificationReceived);
            });
            InitializerThread.Start();

            var InternalCallPrivateKey = ServInit.RequiredEnvironmentVariables["INTERNAL_CALL_PRIVATE_KEY"];

            /*
            * Web-http service initialization
            */
            var WebServiceEndpoints = new List<BWebPrefixStructure>()
            {
                new BWebPrefixStructure(new string[] { RootPath + "3d/process/internal/vm_heartbeat*" }, () => new InternalCalls.VMHeartbeat(ServInit.DatabaseService, InternalCallPrivateKey)),
                new BWebPrefixStructure(new string[] { RootPath + "3d/process/internal/vm_health_check*" }, () => new InternalCalls.VMHealthCheck(ServInit.DatabaseService, ServInit.VMService, VirtualMachineDictionary, InternalCallPrivateKey)),
                new BWebPrefixStructure(new string[] { RootPath + "3d/process/start" }, () => new StartProcessRequest(ServInit.DatabaseService)),
                new BWebPrefixStructure(new string[] { RootPath + "3d/process/stop" }, () => new StopProcessRequest(ServInit.DatabaseService, ServInit.VMService)),
                new BWebPrefixStructure(new string[] { RootPath + "3d/process/internal/job-complete/*" }, () => new BatchJobCompleteRequest(ServInit.DatabaseService, ServInit.FileService, ServInit.MemoryService)),
                new BWebPrefixStructure(new string[] { RootPath + "3d/process/internal/get_file_optimizer_parameters/*" }, () => new GetOptimizerParametersRequest(ServInit.DatabaseService))
            };
            var BWebService = new BWebService(WebServiceEndpoints.ToArray(), ServInit.ServerPort/*, ServInit.TracingService*/);
            BWebService.Run((string Message) =>
            {
                ServInit.LoggingService.WriteLogs(BLoggingServiceMessageUtility.Single(EBLoggingServiceLogType.Info, Message), ServInit.ProgramID, "WebService");
            });

            /*
            * Make main thread sleep forever
            */
            Thread.Sleep(Timeout.Infinite);
        }
    }
}