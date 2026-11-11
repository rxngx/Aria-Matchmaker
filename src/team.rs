use crate::state::PlayerInfo;

pub fn build_teams(players: &[PlayerInfo], playlist: &str, max_players: u32) -> Vec<Vec<Vec<String>>> {
    let squad_size = squad_size_for(playlist);
    let mut teams: Vec<Vec<Vec<String>>> = Vec::new();
    let mut assigned: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut total = 0u32;

    let non_fill: Vec<&PlayerInfo> = players.iter().filter(|p| !p.is_fill).collect();
    let fill: Vec<&PlayerInfo> = players.iter().filter(|p| p.is_fill).collect();

    for player in &non_fill {
        if assigned.contains(&player.account_id) || total >= max_players {
            continue;
        }

        let party_members: Vec<String> = player
            .party
            .split(',')
            .filter(|id| !id.is_empty())
            .map(|s| s.to_string())
            .collect();

        let valid_party: Vec<String> = party_members
            .iter()
            .filter(|id| !assigned.contains(*id))
            .cloned()
            .collect();

        if valid_party.is_empty() {
            continue;
        }

        let team_members: Vec<String> = valid_party
            .into_iter()
            .take(squad_size as usize)
            .collect();

        total += team_members.len() as u32;

        for id in &team_members {
            assigned.insert(id.clone());
        }

        teams.push(vec![team_members]);
    }

    let mut fill_team: Vec<String> = Vec::new();
    for player in &fill {
        if assigned.contains(&player.account_id) || total >= max_players {
            continue;
        }
        fill_team.push(player.account_id.clone());
        assigned.insert(player.account_id.clone());
        total += 1;

        if fill_team.len() >= squad_size as usize {
            teams.push(vec![fill_team.clone()]);
            fill_team.clear();
        }
    }

    if !fill_team.is_empty() {
        teams.push(vec![fill_team]);
    }

    teams
}

pub fn squad_size_for(playlist: &str) -> u32 {
    let p = playlist.to_lowercase();
    if p.contains("duo") {
        2
    } else if p.contains("squad") || p.contains("squads") {
        4
    } else if p.contains("trio") {
        3
    } else {
        1
    }
}

pub fn max_players_for(playlist: &str) -> u32 {
    let p = playlist.to_lowercase();
    if p.contains("playlist_default") || p.contains("50v50") {
        100
    } else {
        100
    }
}
