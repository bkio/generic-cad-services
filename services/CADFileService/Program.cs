/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Threading;
using BCloudServiceUtilities;
using BServiceUtilities;
using BWebServiceUtilities;
using CADFileService.Endpoints;
using CADFileService.Endpoints.Common;
using ServiceUtilities;
using ServiceUtilities.All;

namespace CADFileService
{
    class Program
    {
        static void Main()
        {
            System.Console.WriteLine("Initializing the service...");

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
                    new string[] { "GOOGLE_CLOUD_PROJECT_ID" },
                    new string[] { "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_PLAIN_CREDENTIALS" },

                    new string[] { "DEPLOYMENT_BRANCH_NAME" },
                    new string[] { "DEPLOYMENT_BUILD_NUMBER" },

                    new string[] { "REDIS_ENDPOINT" },
                    new string[] { "REDIS_PORT" },
                    new string[] { "REDIS_PASSWORD" },

                    new string[] { "CAD_FILE_STORAGE_BUCKET" },

                    new string[] { "AUTH_SERVICE_ENDPOINT" },
                    new string[] { "CAD_PROCESS_SERVICE_ENDPOINT" },

                    new string[] { "INTERNAL_CALL_PRIVATE_KEY" }
                }))
                return;
            bool bInitSuccess = true;
            bInitSuccess &= ServInit.WithDatabaseService();
            bInitSuccess &= ServInit.WithFileService();
            bInitSuccess &= ServInit.WithPubSubService();
            bInitSuccess &= ServInit.WithTracingService();
            bInitSuccess &= ServInit.WithMemoryService();
            if (!bInitSuccess) return;

            Resources_DeploymentManager.Get().SetDeploymentBranchNameAndBuildNumber(ServInit.RequiredEnvironmentVariables["DEPLOYMENT_BRANCH_NAME"], ServInit.RequiredEnvironmentVariables["DEPLOYMENT_BUILD_NUMBER"]);

            CommonData.MemoryQueryParameters = new BMemoryQueryParameters()
            {
                Domain = Resources_DeploymentManager.Get().GetDeploymentBranchNameEscapedLoweredWithDash().ToUpper(),
                SubDomain = "COMMON_DATA",
                Identifier = "MEMORY_SERVICE_DATA"
            };

            var AuthServiceEndpoint = ServInit.RequiredEnvironmentVariables["AUTH_SERVICE_ENDPOINT"];

            var CadFileStorageBucketName = ServInit.RequiredEnvironmentVariables["CAD_FILE_STORAGE_BUCKET"];
            var CadProcessServiceEndpoint = ServInit.RequiredEnvironmentVariables["CAD_PROCESS_SERVICE_ENDPOINT"];

            Controller_DeliveryEnsurer.Get().SetDatabaseService(ServInit.DatabaseService);
            Controller_DeliveryEnsurer.Get().SetFileService(ServInit.FileService);
            Controller_DeliveryEnsurer.Get().SetServiceIdentifier("cad-file-service", Actions.EAction.ACTION_CAD_FILE_SERVICE_DELIVERY_ENSURER);
            Controller_AtomicDBOperation.Get().SetMemoryService(ServInit.MemoryService, CommonData.MemoryQueryParameters);

            Manager_PubSubService.Get().Setup(ServInit.PubSubService);

            var InitializerThread = new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;

                ServInit.PubSubService.Subscribe(CommonData.MemoryQueryParameters, Manager_PubSubService.Get().OnMessageReceived_Internal,
                    (string Message) =>
                    {
                        ServInit.LoggingService.WriteLogs(BLoggingServiceMessageUtility.Single(EBLoggingServiceLogType.Error, Message), ServInit.ProgramID, "PubSubService");
                    });

                Controller_AtomicDBOperation.Get().StartTimeoutCheckOperation(WebServiceBaseTimeoutableProcessor.OnTimeoutNotificationReceived);

            });
            InitializerThread.Start();

            var InternalCallPrivateKey = ServInit.RequiredEnvironmentVariables["INTERNAL_CALL_PRIVATE_KEY"];

            /*
            * Web-http service initialization
            */
            var WebServiceEndpoints = new List<BWebPrefixStructure>()
            {
                new BWebPrefixStructure(new string[] { "/file/internal/pubsub*" }, () => new InternalCalls.PubSub_To_CadFileService(InternalCallPrivateKey, ServInit.DatabaseService, ServInit.FileService, CadFileStorageBucketName, CadProcessServiceEndpoint)),
                new BWebPrefixStructure(new string[] { "/file/internal/cleanup*" }, () => new InternalCalls.CleanupCall(InternalCallPrivateKey, ServInit.DatabaseService, ServInit.MemoryService)),
                new BWebPrefixStructure(new string[] { "/file/internal/globally_shared_models*" }, () => new ListGloballySharedModelIds.ForInternal(InternalCallPrivateKey, ServInit.DatabaseService)),
                new BWebPrefixStructure(new string[] { "/file/internal/check_models_exist*" }, () => new InternalCalls.CheckModelsExist(InternalCallPrivateKey, ServInit.DatabaseService)),
                new BWebPrefixStructure(new string[] { "/file/get_models_by/user_id/*/metadata_key/*/metadata_values/*" }, () => new GetModelsBy_MetadataKeyValueUserPair(ServInit.DatabaseService, "user_id", "metadata_key", "metadata_values")),
                new BWebPrefixStructure(new string[] { "/file/get_models_by/user_id/*/metadata_key/*" }, () => new GetModelsBy_MetadataKeyUserPair(ServInit.DatabaseService, "user_id", "metadata_key")),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*/raw" }, () => new Model_GetUpdateDeleteRaw_ForRevisionVersion(ServInit.FileService, ServInit.DatabaseService, "models", "revisions", "versions", CadFileStorageBucketName, CadProcessServiceEndpoint)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*/hierarchy/nodes/*" }, () => new Model_GetHierarchyNode_ForRevisionVersion(ServInit.FileService, ServInit.DatabaseService, "models", "revisions", "versions", "nodes", CadFileStorageBucketName)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*/hierarchy" }, () => new Model_GetHierarchyFile_ForRevisionVersion(ServInit.FileService, ServInit.DatabaseService, "models", "revisions", "versions", CadFileStorageBucketName)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*/geometry/nodes/*" }, () => new Model_GetGeometryNode_ForRevisionVersion(ServInit.FileService, ServInit.DatabaseService, "models", "revisions", "versions", "nodes", CadFileStorageBucketName)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*/geometry" }, () => new Model_GetGeometryFile_ForRevisionVersion(ServInit.FileService, ServInit.DatabaseService, "models", "revisions", "versions", CadFileStorageBucketName)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*/metadata/nodes/*" }, () => new Model_GetMetadataNode_ForRevisionVersion(ServInit.FileService, ServInit.DatabaseService, "models", "revisions", "versions", "keys", CadFileStorageBucketName)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*/metadata" }, () => new Model_GetMetadataFile_ForRevisionVersion(ServInit.FileService, ServInit.DatabaseService, "models", "revisions", "versions", CadFileStorageBucketName)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*" }, () => new Model_GetUpdateDeleteRevisionVersion(ServInit.DatabaseService, "models", "revisions", "versions", CadFileStorageBucketName)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions" }, () => new Model_AddListRevisionVersions(ServInit.DatabaseService, "models", "revisions")),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*" }, () => new Model_GetUpdateDeleteRevision(ServInit.DatabaseService, "models", "revisions", CadFileStorageBucketName)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions" }, () => new Model_AddListRevisions(ServInit.DatabaseService, "models")),
                new BWebPrefixStructure(new string[] { "/file/models/*/sharing" }, () => new Model_ChangeSharingWithUsers(InternalCallPrivateKey, AuthServiceEndpoint, ServInit.DatabaseService, "models")),
                new BWebPrefixStructure(new string[] { "/file/models/*/remove_sharing_from/user_id/*" }, () => new Model_RemoveModelShare(ServInit.DatabaseService, "models", "user_id")),
                new BWebPrefixStructure(new string[] { "/file/models/globally_shared" }, () => new ListGloballySharedModelIds.ForUsers(ServInit.DatabaseService)),
                new BWebPrefixStructure(new string[] { "/file/models/*" }, () => new Model_GetUpdateDeleteModel(ServInit.DatabaseService, "models")),
                new BWebPrefixStructure(new string[] { "/file/models" }, () => new Model_AddListModels(ServInit.DatabaseService)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*/unreal/hierarchy_geometry_metadata" }, () => new Model_GetUnrealHierarchyMetadataGeometry(ServInit.FileService, ServInit.DatabaseService, "models", "revisions", "versions", CadFileStorageBucketName)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*/unreal/hiearchy_geometry" }, () => new Model_GetUnrealHierarchyGeometry(ServInit.FileService, ServInit.DatabaseService, "models", "revisions", "versions", CadFileStorageBucketName)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*/unreal/hiearchy" }, () => new Model_GetUnrealHierarchy(ServInit.FileService, ServInit.DatabaseService, "models", "revisions", "versions", CadFileStorageBucketName)),
                new BWebPrefixStructure(new string[] { "/file/models/*/revisions/*/versions/*/unreal/geometry_files/*" }, () => new Model_GetUnrealGeometry(ServInit.FileService, ServInit.DatabaseService, "models", "revisions", "versions", "geometry_files", CadFileStorageBucketName)),
            };
            var BWebService = new BWebService(WebServiceEndpoints.ToArray(), ServInit.ServerPort, ServInit.TracingService);
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