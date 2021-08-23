import json
from responseTemplate import responseTemplate
from mysql.connector import connect, Error
from awsSecrets import getDbPass

def downAndDistance(event, context):
    body = json.loads(event["body"]);

    keys = body.keys();

    if ("season" not in keys): return responseTemplate(400, "season");
    if ("down" not in keys): return responseTemplate(400, "down");
    if ("distanceLowerBound" not in keys): return responseTemplate(400, "distanceLowerBound");
    if ("distanceUpperBound" not in keys): return responseTemplate(400, "distanceUpperBound");
    
    connection = connect(
        host = 'dfs.cxqsjcdo8n1w.us-east-1.rds.amazonaws.com',
        user = 'GavSwe',
        password = getDbPass(),
        database = 'NFL'
    )
    
    cursor = connection.cursor();

    args = (
        body["season"],
        body["down"],
        body["distanceLowerBound"],
        body["distanceUpperBound"]
    )
    
    result = cursor.callproc("DownAndDistanceTeamTendencies", args);
    for result in cursor.stored_results():
        teamResults = result.fetchall();
        responseBody = [formatTeamResult(teamResult) for teamResult in teamResults];
    
    connection.commit();
    cursor.close();
    connection.close();

    return responseTemplate(200, responseBody);

def formatTeamResult(teamResult):
    return {
        "team": teamResult[0],
        "total": int(teamResult[1]),
        "rush": float(teamResult[2]),
        "pass": float(teamResult[3]),
        "fieldGoal": float(teamResult[4]),
        "punt": float(teamResult[5])
    }
