import json
from responseTemplate import responseTemplate
from mysql.connector import connect, Error
from awsSecrets import getDbPass
from buildPositionBoardWhereClause import buildPositionBoardWhereClause

def runningbackBoard(event, context):
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
        PL.Season,
        PL.PlayerId, 
        PL.PlayerName,
        PST.TeamAbv,
        G.Games,
        SUM(CASE WHEN PL.RushingYardsFlg = 1 THEN PL.Count ELSE 0 END) AS RushingAttempts,
        SUM(CASE WHEN PL.RushingYardsFlg = 1 THEN PL.Yards ELSE 0 END) AS RushingYards,
        SUM(CASE WHEN PL.StatId = 115 THEN PL.Count ELSE 0 END) AS Targets,
        SUM(CASE WHEN PL.ReceivingYardsFlg = 1 THEN PL.Count ELSE 0 END) AS Receptions,
        SUM(CASE WHEN PL.ReceivingYardsFlg = 1 THEN PL.Yards ELSE 0 END) AS ReceivingYards,
        SUM(CASE WHEN PL.StatId = 113 THEN PL.Yards ELSE 0 END) AS YardsAfterCatch,
        SUM(CASE WHEN PL.RushingYardsFlg = 1 OR PL.ReceivingYardsFlg = 1 THEN PL.Yards ELSE 0 END) AS TotalYards,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.RushingYardsFlg = 1 THEN PL.Count ELSE 0 END) / 
                G.Games
            , 1) 
        ,0) AS RushingAttemptsPerGame,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.RushingYardsFlg = 1 THEN PL.Yards ELSE 0 END) /
                SUM(CASE WHEN PL.RushingYardsFlg = 1 THEN PL.Count ELSE 0 END)
            , 1)
        , 0) AS RushingYardsPerAttempt,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.StatId = 115 THEN PL.Count ELSE 0 END) /
                G.Games
            , 1) 
        , 0) AS TargetsPerGame,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.ReceivingYardsFlg = 1 THEN PL.Count ELSE 0 END) /
                SUM(CASE WHEN PL.StatId = 115 THEN PL.Count ELSE 0 END)
            , 1) 
        , 0) AS ReceptionsPerTarget,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.ReceivingYardsFlg = 1 THEN PL.Yards ELSE 0 END) / 
                SUM(CASE WHEN PL.ReceivingYardsFlg = 1 THEN PL.Count ELSE 0 END)
            , 1) 
        , 0) AS YardsPerReception,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.StatId = 113 THEN PL.Yards ELSE 0 END) / 
                SUM(CASE WHEN PL.ReceivingYardsFlg = 1 THEN PL.Count ELSE 0 END)
            , 1)
        , 0) AS YardsAfterCatchPerReception,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.RushingYardsFlg = 1 OR PL.ReceivingYardsFlg = 1 THEN PL.Yards ELSE 0 END) /
                G.Games
            , 1) 
        , 0) AS TotalYardsPerGame

        FROM
        (
            SELECT 
            P.PlayerId,
            PS.PlayerName,
            G.Season,
            COUNT(DISTINCT(G.GameId)) AS G,
            COUNT(PS.StatId) AS Count,
            SUM(PS.Yards) AS Yards,
            S.*
            FROM NFL.PlayStats PS 
            LEFT JOIN NFL.Games G ON PS.GameDetailId = G.GameDetailId
            LEFT JOIN NFL.StatLk S ON PS.StatId = S.StatId
            LEFT JOIN NFL.Players P ON PS.PlayerGsisId = P.GsisId
            WHERE 
            {whereClauses["seasonClause"]}
            {whereClauses["seasonTypeClause"]} 
            {whereClauses["weekClause"]}
            {whereClauses["teamClause"]}
            AND S.RunningBack = 1
            GROUP BY P.PlayerId, G.Season, PS.StatId
            ORDER BY P.PlayerId, G.Season, PS.StatId
        ) PL
        LEFT JOIN (
            SELECT 
            P.PlayerId,
            COUNT(DISTINCT(G.GameId)) AS Games
            FROM NFL.PlayStats PS 
            LEFT JOIN NFL.Games G ON PS.GameDetailId = G.GameDetailId
            LEFT JOIN NFL.Players P ON PS.PlayerGsisId = P.GsisId
            WHERE 
            {whereClauses["seasonClause"]}
            {whereClauses["seasonTypeClause"]}
            {whereClauses["weekClause"]}
            {whereClauses["teamClause"]}
            AND PS.PlayerGsisId IS NOT NULL
            GROUP BY P.PlayerId
        ) G ON PL.PlayerId = G.PlayerId
        LEFT JOIN NFL.PlayerSeasonTeams PST ON PL.PlayerId = PST.PlayerId AND PL.Season = PST.Season
        GROUP BY PL.PlayerId
        HAVING SUM(CASE WHEN PL.RushingYardsFlg = 1 THEN PL.Count ELSE 0 END) / G.Games > 6.25
        ORDER BY SUM(CASE WHEN PL.RushingYardsFlg = 1 OR PL.ReceivingYardsFlg = 1 THEN PL.Yards ELSE 0 END) DESC
    """;

    cursor.execute(query);

    responseBody = [formatResult(row) for row in cursor];

    cursor.close();
    connection.close();

    return responseTemplate(200, responseBody);

def formatResult(row):
    return {    
        "season": int(row[0]),
        "playerId": int(row[1]),
        "playerName": str(row[2]),
        "team": str(row[3]),
        "games": int(row[4]),
        "rushingAttempts": int(row[5]),
        "rushingYards": int(row[6]),
        "targets": int(row[7]),
        "receptions": int(row[8]),
        "receivingYards": int(row[9]),
        "yardsAfterCatch": int(row[10]),
        "totalYards": int(row[11]),
        "rushingAttemptsPerGame": float(row[12]),
        "rushingYardsPerAttempt": float(row[13]),
        "targetsPerGame": float(row[14]),
        "receptionsPerTarget": float(row[15]),
        "yardsPerReception": float(row[16]),
        "yardsAfterCatchPerReception": float(row[17]),
        "totalYardsPerGame": float(row[18])
    }

event = {
    "body": json.dumps({
        "season": 2020,
        "seasonType": ["REG","POST"],
        # "weeks": [1,2,3]
    })
}
res = runningbackBoard(event, {});

print(res);
