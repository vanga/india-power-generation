# simple AWS lambda function to run on AWS Cloud (Mumbai region) since meritindia.in is not accessible outside India.
import json
import daily_generation_helper as dgh
import current_generation_helper as cgh


def lambda_handler(event, context):
    assert "body" in event, "Request does not contain body"
    req_body = json.loads(event["body"])

    assert "type" in req_body, "Request body does not contain type"
    match req_body["type"]:
        case "current-state-generation":
            rows = cgh.get_data()
        case "current-india-generation":
            rows = cgh.get_india_row()
        case "daily-state-generation":
            assert "inputs" in req_body, "Request body does not contain inputs"
            rows = dgh.get_data("daily-state-generation", req_body["inputs"])
        case "daily-plant-generation":
            assert "inputs" in req_body, "Request body does not contain inputs"
            rows = dgh.get_data("daily-plant-generation", req_body["inputs"])
        case _:
            raise ValueError(f"Unknown type: {req_body['type']}")
    return {"statusCode": 200, "body": {"data": rows}}
