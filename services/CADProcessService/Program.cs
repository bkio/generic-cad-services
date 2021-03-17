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

namespace CADProcessService
{
    class Program
    {
        static void Main()
        {
            Console.WriteLine("Initializing the service...");

#if (Debug || DEBUG)
            if (!ServicesDebugOnlyUtilities.CalledFromMain()) return;
#endif

            // In case of a cloud component dependency or environment variable is added/removed;

            /*
            * Common initialization step
            */
            if (!BServiceInitializer.Initialize(out BServiceInitializer ServInit,
                new string[][]
                {
                    new string[] { "MONGO_DB_CONNECTION_STRING" },
                    new string[] { "MONGO_DB_DATABASE" },

                    new string[] { "DEPLOYMENT_BRANCH_NAME" },
                    new string[] { "DEPLOYMENT_BUILD_NUMBER" },

                    new string[] { "REDIS_ENDPOINT" },
                    new string[] { "REDIS_PORT" },
                    new string[] { "REDIS_PASSWORD" },

                    new string[] { "CAD_PROCESS_SERVICE_NAME" },
                    new string[] { "CAD_PROCESS_POD_NAME" },
                    new string[] { "CAD_PROCESS_PORT" },

                    new string[] { "CLUSTER_MASTER_ENDPOINT" },
                    new string[] { "CLUSTER_CLIENT_KEY" },
                    new string[] { "CLUSTER_CLIENT_CERTIFICATE" },

                    new string[] { "CAD_READER_IMAGE" },
                    new string[] { "FILE_WORKER_IMAGE" },
                    new string[] { "FILE_OPTIMIZER_IMAGE" }
                }))
                return;

            bool bInitSuccess = true;
            bInitSuccess &= ServInit.WithDatabaseService();
            bInitSuccess &= ServInit.WithFileService();
            //bInitSuccess &= ServInit.WithTracingService();
            bInitSuccess &= ServInit.WithPubSubService();
            bInitSuccess &= ServInit.WithMemoryService();
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

                BatchProcessingCreationService.Initialize(ServInit,
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

            /*
            * Web-http service initialization
            */
            var WebServiceEndpoints = new List<BWebPrefixStructure>()
            {
                new BWebPrefixStructure(new string[] { "/3d/process/start" }, () => new StartProcessRequest(ServInit.DatabaseService)),
                new BWebPrefixStructure(new string[] { "/3d/process/stop" }, () => new StopProcessRequest(ServInit.DatabaseService, ServInit.FileService)),
                new BWebPrefixStructure(new string[] { "/3d/process/internal/job-complete/*" }, () => new BatchJobCompleteRequest(ServInit.DatabaseService, ServInit.FileService, ServInit.MemoryService)),
                new BWebPrefixStructure(new string[] { "/3d/process/internal/get_signed_upload_url_for_unreal_file/*" }, () => new GetSignedUploadUrlRequest(ServInit.FileService)),
                new BWebPrefixStructure(new string[] { "/3d/process/internal/get_file_optimizer_parameters/*" }, () => new GetOptimizerParametersRequest(ServInit.DatabaseService))
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