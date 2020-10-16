/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using ServiceUtilities;
using Newtonsoft.Json;

namespace CADFileService.Controllers
{
    public class Controller_ModelActions
    {
        private static Controller_ModelActions Instance = null;
        private Controller_ModelActions() { }
        public static Controller_ModelActions Get()
        {
            if (Instance == null)
            {
                Instance = new Controller_ModelActions();
            }
            return Instance;
        }

        public bool BroadcastModelAction(Action_ModelAction _Action, Action<string> _ErrorMessageAction = null)
        {
            if (_Action == null)
            {
                _ErrorMessageAction?.Invoke("Controller_ModelActions->BroadcastModelAction: Action input is null.");
                return false;
            }

            return Manager_PubSubService.Get().PublishAction(
                _Action.GetActionType(),
                JsonConvert.SerializeObject(_Action));
        }
    }
}
