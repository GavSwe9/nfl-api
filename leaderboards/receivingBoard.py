import json
from responseTemplate import responseTemplate
from mysql.connector import connect, Error
from awsSecrets import getDbPass
from buildPositionBoardWhereClause import buildPositionBoardWhereClause

def receivingBoard(event, context):
    body = json.loads(event["body"]);

    keys = body.keys();

    # Check for the two required fields 
    if ("season" not in keys): return responseTemplate(400, "season");
    if ("seasonType" not in keys): return responseTemplate(400, "season");
    
    whereClauses = buildPositionBoardWhereClause(body, keys);

    connection = connect(
        host = 'dfs.cxqsjcdo8n1w.us-east-1.rds.amazonaws.com',
        user = 'GavSwe',
        password = getDbPass(),
        database = 'NFL'
    )
    
    cursor = connection.cursor();

    query = f"""
        SELECT
        L.PlayerId,
        P.Name,
        L.TeamAbv,
        L.Season,
        SUM(L.Games) AS Games,

        COALESCE(SUM(L.Targets), 0) AS Targets,
        COALESCE(SUM(L.Receptions), 0) AS Receptions,
        COALESCE(SUM(L.ReceivingYards), 0) AS ReceivingYards,
        COALESCE(SUM(L.YardsAfterCatch), 0) AS YardsAfterCatch,

        COALESCE(
            ROUND(
                SUM(L.Targets) /
                SUM(L.Games)
            , 1) 
        , 0) AS TargetsPerGame,

        COALESCE(
            ROUND(
                SUM(L.Receptions) /
                SUM(L.Targets)
            , 1) 
        , 0) AS ReceptionsPerTarget,

        COALESCE(
            ROUND(
                SUM(L.ReceivingYards) / 
                SUM(L.Receptions)
            , 1) 
        , 0) AS YardsPerReception,

        COALESCE(
            ROUND(
                SUM(L.YardsAfterCatch) / 
                SUM(L.Receptions)
            , 1)
        , 0) AS YardsAfterCatchPerReception

        FROM NFL.GameLogsReceiving L
        LEFT JOIN NFL.PlayerSeasonTeams PST ON L.PlayerId = PST.PlayerId AND L.Season = PST.Season
        LEFT JOIN NFL.Players P on L.PlayerId = P.PlayerId
        WHERE
        {whereClauses["seasonClause"]}
        {whereClauses["seasonTypeClause"]} 
        {whereClauses["weekClause"]}
        {whereClauses["teamClause"]}
        GROUP BY L.PlayerId, L.Season
        HAVING SUM(L.Receptions) / SUM(L.Games) > 1.875
        ORDER BY SUM(L.ReceivingYards) DESC
    """;

    cursor.execute(query);

    responseBody = [formatResult(row) for row in cursor];

    cursor.close();
    connection.close();

    return responseTemplate(200, responseBody);

def formatResult(row):
    return {    
        "playerId": int(row[0]),
        "playerName": str(row[1]),
        "team": str(row[2]),
        "season": int(row[3]),
        "games": int(row[4]),
        "targets": int(row[5]),
        "receptions": int(row[6]),
        "receivingYards": int(row[7]),
        "yardsAfterCatch": int(row[8]),
        "targetsPerGame": float(row[9]),
        "receptionsPerTarget": float(row[10]),
        "yardsPerReception": float(row[11]),
        "yardsAfterCatchPerReception": float(row[12])
    }

# event = {
#     "body": json.dumps({
#         "season": 2020,
#         "seasonType": ["REG","POST"],
#         # "weeks": [1,2,3],
#         "teams": ["CIN", "NE"]
#     })
# }
# res = receiverBoard(event, {});

# print(res);
