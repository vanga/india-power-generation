import daily_generation_helper as daily_generation_helper
import current_generation_helper as current_generation_helper


def lambda_handler(event, context):
    req_body = event["body"]
    assert req_body, "Request body is empty"
    assert "type" in req_body, "Request body does not contain type"
    match req_body["type"]:
        case "realtime_state_generation":
            rows = current_generation_helper.get_data()
        case "daily_state_generation":
            assert "inputs" in req_body, "Request body does not contain inputs"
            rows = daily_generation_helper.get_data(req_body["inputs"])
        case _:
            raise ValueError(f"Unknown type: {req_body['type']}")
    return {"statusCode": 200, "body": {"data": rows}}
