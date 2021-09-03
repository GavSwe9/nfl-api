import json
from responseTemplate import responseTemplate
from mysql.connector import connect, Error
from awsSecrets import getDbPass
from buildPositionBoardWhereClause import buildPositionBoardWhereClause

def passingBoard(event, context):
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
        SUM(L.Games) Games,
        SUM(L.Attempts) Attempts,
        SUM(L.Completions) Completions,
        SUM(L.PassingYards) PassingYards,
        SUM(L.Interceptions) Interceptions,
        SUM(L.AirYardsAttempted) AirYardsAttempted,
        SUM(L.AirYardsCompleted) AirYardsCompleted,

        COALESCE(
            ROUND(
                SUM(L.Attempts) /
                SUM(L.Games)
            , 2)
        , 0) AS AttemptsPerGame,

        COALESCE(
            ROUND(
                SUM(L.Completions) / 
                SUM(L.Games)
            , 2)
        , 0) AS CompletionsPerGame,

        COALESCE(
            ROUND(
                SUM(L.Completions) /
                SUM(L.Attempts) * 100
            , 1)
        , 0) AS CompletionsPerAttempts,

        COALESCE(
            ROUND(
                SUM(L.PassingYards) / 
                SUM(L.Games)
            , 2)
        , 0) AS PassingYardsPerGame,

        COALESCE(
            ROUND(
                SUM(L.Interceptions) / 
                SUM(L.Games)
            , 2)
        , 0) AS InterceptionsPerGame,

        COALESCE(
            ROUND(
                SUM(L.AirYardsAttempted) / 
                SUM(L.Games)
            , 2)
        , 0) AS AirYardsAttemptedPerGame,

        COALESCE(
            ROUND(
                SUM(L.AirYardsCompleted) / 
                SUM(L.Games)
            , 2)
        , 0) AS AirYardsCompletedPerGame,

        COALESCE(
            ROUND(
                SUM(L.AirYardsCompleted) / 
                SUM(L.AirYardsAttempted) *100
            , 1)
        , 0) AS AirYardCompletionPercentage,

        COALESCE(SUM(GLRS.RushingAttempts), 0) AS RushingAttempts,
        COALESCE(SUM(GLRS.RushingYards), 0) AS RushingYards,

        COALESCE(
            ROUND(
                SUM(GLRS.RushingAttempts) / 
                SUM(L.Games)
            , 2)
        , 0) AS RushingAttemptsPerGame,

        COALESCE(
            ROUND(
                SUM(GLRS.RushingYards) / 
                SUM(L.Games)
            , 2)
        , 0) AS RushingYardsPerGame,

        COALESCE(
            ROUND(
                SUM(GLRS.RushingAttempts) / 
                SUM(L.Games)
            , 2)
        , 0) AS RushingYardsPerAttempt,

        COALESCE(SUM(L.PassingYards), 0) + COALESCE(SUM(GLRS.RushingYards), 0) AS TotalYards,

        COALESCE(
            ROUND(
                (COALESCE(SUM(L.PassingYards), 0) + COALESCE(SUM(GLRS.RushingYards), 0)) / 
                SUM(L.Games)
            , 2)
        , 0) AS TotalYardsPerGame

        FROM NFL.GameLogsPassing L
        LEFT JOIN NFL.GameLogsRushing GLRS ON L.PlayerId = GLRS.PlayerId AND L.GameId = GLRS.GameId
        LEFT JOIN NFL.PlayerSeasonTeams PST ON L.PlayerId = PST.PlayerId AND L.Season = PST.Season
        LEFT JOIN NFL.Players P on L.PlayerId = P.PlayerId
        WHERE
        {whereClauses["seasonClause"]}
        {whereClauses["seasonTypeClause"]} 
        {whereClauses["weekClause"]}
        {whereClauses["teamClause"]}
        GROUP BY L.PlayerId, L.Season
        HAVING SUM(L.Attempts) / SUM(L.Games) > 14
        ORDER BY SUM(L.PassingYards) DESC
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
        "attempts": int(row[5]),
        "completions": int(row[6]),
        "passingYards": int(row[7]),
        "interceptions": int(row[8]),
        "airYardsAttempted": int(row[9]),
        "airYardsCompleted": int(row[10]),
        "attemptsPerGame": float(row[11]),
        "completionsPerGame": float(row[12]),
        "completionsPerAttempts": float(row[13]),
        "passingYardsPerGame": float(row[14]),
        "interceptionsPerGame": float(row[15]),
        "airYardsAttemptedPerGame": float(row[16]),
        "airYardsCompletedPerGame": float(row[17]),
        "airYardCompletionPercentage": float(row[18]),
        "rushingAttempts": int(row[19]),
        "rushingYards": int(row[20]),
        "rushingAttemptsPerGame": float(row[21]),
        "rushingYardsPerGame": float(row[22]),
        "rushingYardsPerAttempt": float(row[23]),
        "totalYards": int(row[24])
    }

# event = {
#     "body": json.dumps({
#         "season": 2020,
#         "seasonType": ["REG","POST"],
#         # "weeks": [1,2,3]
#     })
# }
# res = quarterbackBoard(event, {});

# print(res);
