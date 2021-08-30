
def buildPositionBoardWhereClause(body, keys):
    # Build Season Where Clause
    season = body["season"];
    seasonClause = f"L.Season = {season}";

    # Build SeasonType Where Clause
    seasonTypeList = ",".join("'" + st + "'" for st in body["seasonType"]);
    seasonTypeClause = f"AND L.SeasonType IN ({seasonTypeList})";

    # Build Week Where Clause
    weekClause = "";
    if ("weeks" in keys):
        # Note: Including "Weeks" assumes REG season and will override any additional seasonType
        seasonTypeClause = "AND L.SeasonType = 'Reg'";
        
        weeksList = ",".join(str(w) for w in body["weeks"]);
        weekClause = f"AND L.Week IN ({weeksList})";
    
    # Build Team Where Clause
    teamClause = "";
    if ("teams" in keys):
        teamList = ",".join("'" + str(w) + "'" for w in body["teams"]);
        teamClause = f"AND L.TeamAbv IN ({teamList})";
    
    return {
        "seasonClause": seasonClause,
        "seasonTypeClause": seasonTypeClause,
        "weekClause": weekClause,
        "teamClause": teamClause
    }
    