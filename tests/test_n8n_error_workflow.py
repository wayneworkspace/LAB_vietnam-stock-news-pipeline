import json
import os

PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "n8n", "error-alert.workflow.json")


def test_error_workflow_wires_the_error_trigger_to_discord_and_holds_no_secret():
    text = open(PATH, encoding="utf-8").read()
    wf = json.loads(text)
    types = {n["name"]: n["type"] for n in wf["nodes"]}
    assert sorted(types.values()) == ["n8n-nodes-base.discord", "n8n-nodes-base.errorTrigger"]
    trigger = next(n for n, t in types.items() if t.endswith("errorTrigger"))
    discord = next(n for n, t in types.items() if t.endswith("discord"))
    assert wf["connections"][trigger]["main"][0][0]["node"] == discord
    assert "discord.com/api/webhooks" not in text      # the webhook URL belongs in an n8n credential
    assert "credentials" not in text                   # picked after import, so no credential ids are committed
