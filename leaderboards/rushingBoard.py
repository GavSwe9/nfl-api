import json
from responseTemplate import responseTemplate
from mysql.connector import connect, Error
from awsSecrets import getDbPass
from buildPositionBoardWhereClause import buildPositionBoardWhereClause

def rushingBoard(event, context):
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
        SUM(L.RushingAttempts) AS RushingAttempts,
        SUM(L.RushingYards) AS RushingYards,

        COALESCE(
            ROUND(
                SUM(L.RushingAttempts) / 
                SUM(L.Games)
            , 2)
        , 0) AS RushingAttemptsPerGame,

        COALESCE(
            ROUND(
                SUM(L.RushingYards) / 
                SUM(L.Games)
            , 2)
        , 0) AS RushingYardsPerGame,

        COALESCE(
            ROUND(
                SUM(L.RushingAttempts) / 
                SUM(L.Games)
            , 2)
        , 0) AS RushingYardsPerAttempt,


        COALESCE(SUM(GLRC.Targets), 0) AS Targets,
        COALESCE(SUM(GLRC.Receptions), 0) AS Receptions,
        COALESCE(SUM(GLRC.ReceivingYards), 0) AS ReceivingYards,
        COALESCE(SUM(GLRC.YardsAfterCatch), 0) AS YardsAfterCatch,

        COALESCE(
            ROUND(
                SUM(GLRC.Targets) /
                SUM(L.Games)
            , 1) 
        , 0) AS TargetsPerGame,

        COALESCE(
            ROUND(
                SUM(GLRC.Receptions) /
                SUM(GLRC.Targets)
            , 1) 
        , 0) AS ReceptionsPerTarget,

        COALESCE(
            ROUND(
                SUM(GLRC.ReceivingYards) / 
                SUM(GLRC.Receptions)
            , 1) 
        , 0) AS YardsPerReception,

        COALESCE(
            ROUND(
                SUM(GLRC.YardsAfterCatch) / 
                SUM(GLRC.Receptions)
            , 1)
        , 0) AS YardsAfterCatchPerReception,


        SUM(L.RushingYards) + COALESCE(SUM(GLRC.ReceivingYards), 0) AS TotalYards,

        COALESCE(
            ROUND(
                (SUM(L.RushingYards) + SUM(GLRC.ReceivingYards))/
                SUM(L.Games)
            , 1) 
        , 0) AS TotalYardsPerGame

        FROM NFL.GameLogsRushing L
        LEFT JOIN NFL.GameLogsReceiving GLRC on L.PlayerId = GLRC.PlayerId AND L.GameId = GLRC.GameId 
        LEFT JOIN NFL.PlayerSeasonTeams PST ON L.PlayerId = PST.PlayerId AND L.Season = PST.Season
        LEFT JOIN NFL.Players P on L.PlayerId = P.PlayerId
        WHERE
        {whereClauses["seasonClause"]}
        {whereClauses["seasonTypeClause"]} 
        {whereClauses["weekClause"]}
        {whereClauses["teamClause"]}
        GROUP BY L.PlayerId, L.Season
        HAVING SUM(L.RushingAttempts) / SUM(L.Games) > 6.25
        ORDER BY SUM(L.RushingYards) DESC
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

        "rushingAttempts": int(row[5]),
        "rushingYards": int(row[6]),
        
        "rushingAttemptsPerGame": float(row[7]),
        "rushingYardsPerGame": float(row[8]),
        "rushingYardsPerAttempt": float(row[9]),

        "targets": int(row[10]),
        "receptions": int(row[11]),
        "receivingYards": int(row[12]),
        "yardsAfterCatch": int(row[13]),

        "targetsPerGame": float(row[14]),
        "receptionsPerTarget": float(row[15]),
        "yardsPerReception": float(row[16]),
        "yardsAfterCatchPerReception": float(row[17]),
        
        "totalYards": int(row[18]),
        "totalYardsPerGame": float(row[19])
    }

event = {
    "body": json.dumps({
        "season": 2020,
        "seasonType": ["REG","POST"],
        # "weeks": [1,2,3]
    })
}
res = rushingBoard(event, {});

print(res);
