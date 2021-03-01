/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using ServiceUtilities;
using Newtonsoft.Json;

namespace CADProcessService.Endpoints.Controllers
{
    class Controller_BatchProcess
    {
        private static Controller_BatchProcess Instance = null;
        private Controller_BatchProcess() { }
        public static Controller_BatchProcess Get()
        {
            if (Instance == null)
            {
                Instance = new Controller_BatchProcess();
            }
            return Instance;
        }

        public bool BroadcastBatchProcessAction(Action_BatchProcessAction _Action, Action<string> _ErrorMessageAction = null)
        {
            if (_Action == null)
            {
                _ErrorMessageAction?.Invoke("Controller_BatchProcess->BroadcastUserAction: Action input is null.");
                return false;
            }

            return Manager_PubSubService.Get().PublishAction(
                _Action.GetActionType(),
                JsonConvert.SerializeObject(_Action));
        }
    }
}
