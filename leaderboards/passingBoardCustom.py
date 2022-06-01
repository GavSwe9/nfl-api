import json
from responseTemplate import responseTemplate
from mysql.connector import connect, Error
from awsSecrets import getDbPass
from buildPositionBoardWhereClause import buildPositionBoardWhereClause

def quarterbackBoard(event, context):
    body = json.loads(event["body"]);

    keys = body.keys();

    # Check for the two required fields 
    if ("season" not in keys): return responseTemplate(400, "season");
    if ("seasonType" not in keys): return responseTemplate(400, "season");
    
    whereClauses = buildPositionBoardWhereClause(body, keys);

    connection = connect(
        host = 'farm.cxqsjcdo8n1w.us-east-1.rds.amazonaws.com',
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
        SUM(CASE WHEN PL.PassingYardsFlg = 1 OR PL.StatId = 14 THEN PL.Count ELSE 0 END) AS Attempts,
        SUM(CASE WHEN PL.PassingYardsFlg = 1 THEN PL.Count ELSE 0 END) AS Completions,
        SUM(CASE WHEN PL.PassingYardsFlg = 1 THEN PL.Yards ELSE 0 END) AS PassingYards,
        SUM(CASE WHEN PL.StatId = 19 THEN PL.Count ELSE 0 END) AS Interceptions,
        SUM(CASE WHEN PL.StatId IN (111,112) THEN PL.Count ELSE 0 END) AS AirYardsAttempted,
        SUM(CASE WHEN PL.StatId = 111 THEN PL.Count ELSE 0 END) AS AirYardsCompleted,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.PassingYardsFlg = 1 OR PL.StatId = 14 THEN PL.Count ELSE 0 END) /
                G.Games 
            , 2)
        , 0) AS AttemptsPerGame,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.PassingYardsFlg = 1 THEN PL.Count ELSE 0 END) / 
                G.Games 
            , 2)
        , 0) AS CompletionsPerGame,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.PassingYardsFlg = 1 THEN PL.Count ELSE 0 END) /
                SUM(CASE WHEN PL.PassingYardsFlg = 1 OR PL.StatId = 14 THEN PL.Count ELSE 0 END)
            , 2)
        , 0) AS CompletionsPerAttempts,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.PassingYardsFlg = 1 THEN PL.Yards ELSE 0 END) / 
                G.Games 
            , 2)
        , 0) AS PassingYardsPerGame,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.StatId = 19 THEN PL.Count ELSE 0 END) / 
                G.Games 
            , 2)
        , 0) AS InterceptionsPerGame,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.StatId IN (111,112) THEN PL.Count ELSE 0 END) / 
                G.Games 
            , 2)
        , 0) AS AirYardsAttemptedPerGame,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.StatId = 111 THEN PL.Count ELSE 0 END) / 
                G.Games 
            , 2)
        , 0) AS AirYardsCompletedPerGame,

        COALESCE(
            ROUND(
                SUM(CASE WHEN PL.StatId = 111 THEN PL.Count ELSE 0 END) / 
                SUM(CASE WHEN PL.StatId IN (111,112) THEN PL.Count ELSE 0 END)
            , 2)
        , 0) AS AirYardCompletionPercengage,

        SUM(CASE WHEN PL.RushingYardsFlg = 1 THEN PL.Count ELSE 0 END) AS RushingAttempts,
        SUM(CASE WHEN PL.RushingYardsFlg = 1 THEN PL.Yards ELSE 0 END) AS RushingYards,

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
        , 0) AS RushingYardsPerAttempt

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
            AND S.Quarterback = 1 
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
        HAVING SUM(CASE WHEN PL.PassingYardsFlg = 1 THEN PL.Count ELSE 0 END) / G.Games > 14
        ORDER BY SUM(CASE WHEN PL.PassingYardsFlg = 1 THEN PL.Yards ELSE 0 END) DESC
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
        "attempts": int(row[5]),
        "completions": int(row[6]),
        "passingYards": int(row[7]),
        "interceptions": int(row[8]),
        "airYardsAttempted": int(row[9]),
        "airYardsCompleted": int(row[10]),
        "attemptsPerGame": float(row[11]),
        "completionsPerGame": float(row[12]),
        "CompletionsPerAttempts": float(row[13]),
        "PassingYardsPerGame": float(row[14]),
        "InterceptionsPerGame": float(row[15]),
        "AirYardsAttemptedPerGame": float(row[16]),
        "AirYardsCompletedPerGame": float(row[17]),
        "AirYardCompletionPercentage": float(row[18]),
        "RushingAttempts": float(row[19]),
        "RushingYards": float(row[20]),
        "RushingAttemptsPerGame": float(row[21]),
        "RushingYardsPerAttempt": float(row[22])
    }

# event = {
#     "body": json.dumps({
#         "season": 2020,
#         "seasonType": ["'REG'","'POST'"],
#         # "weeks": [1,2,3]
#     })
# }
# res = quarterbackBoard(event, {});

# print(res);
