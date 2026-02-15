#!/usr/bin/env python3
"""
Fetch NWO project data and extract collaboration information.
Queries ROR API for institution coordinates and outputs JSON for visualization.
"""

import json
import time
import requests
from collections import defaultdict
from itertools import combinations

# API endpoints
NWO_API = "https://nwopen-api.nwo.nl/NWOpen-API/api/Projects"
ROR_API = "https://api.ror.org/organizations"

def fetch_nwo_projects(limit=50, reporting_years=[2024, 2025]):
    """Fetch projects from NWO Open API for multiple years, only with DOI."""
    all_projects = []

    for year in reporting_years:
        print(f"Fetching from {year}...")
        params = {"page": 1, "pageSize": 100, "reporting_year": year}

        while len(all_projects) < limit:
            try:
                response = requests.get(NWO_API, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                projects = data.get("projects", [])

                if not projects:
                    break

                # Only keep projects with DOI and from Regieorgaan Open Science
                for p in projects:
                    if p.get("grant_id") and p.get("sub_department") == "Regieorgaan Open Science" and len(all_projects) < limit:
                        all_projects.append(p)

                print(f"  Found {len(all_projects)} projects with DOI...")

                if len(projects) < 100:
                    break
                params["page"] += 1
                time.sleep(0.3)
            except requests.exceptions.RequestException as e:
                print(f"  Error: {e}")
                break

        if len(all_projects) >= limit:
            break

    return all_projects[:limit]

def extract_collaborations(projects):
    """Extract institution collaborations from projects."""
    print("Extracting collaborations...")

    institution_projects = defaultdict(list)  # ror_id -> list of {id, title}
    # collaboration_pairs now stores: {(ror1, ror2): {scheme: count, ...}}
    collaboration_pairs = defaultdict(lambda: defaultdict(int))
    funding_schemes = set()
    all_projects_list = []  # All projects regardless of ROR
    projects_with_grant_id = 0

    for project in projects:
        grant_id = project.get("grant_id", "")  # DOI link
        project_title = project.get("title", "Untitled")

        # Skip projects without a valid grant_id (DOI)
        if not grant_id or grant_id.strip() == "":
            continue

        projects_with_grant_id += 1
        funding_scheme = project.get("funding_scheme", "Unknown")
        funding_schemes.add(funding_scheme)
        project_info = {"grant_id": grant_id, "title": project_title[:60], "funding_scheme": funding_scheme}

        # Add to all projects list
        all_projects_list.append(project_info)

        # Get project members with ROR IDs
        members = project.get("project_members", project.get("projectMembers", []))

        # Extract unique ROR IDs from this project
        ror_ids = set()
        if members:
            for member in members:
                ror = member.get("ror") or member.get("rorId") or member.get("institution_ror")
                if ror and ror != "-" and "ror.org/" in ror:
                    ror = ror.split("ror.org/")[-1]
                    ror_ids.add(ror)

        # Store project info for each institution (if any ROR IDs found)
        for ror in ror_ids:
            if not any(p["grant_id"] == grant_id for p in institution_projects[ror]):
                institution_projects[ror].append(project_info)

        # Count collaboration pairs by funding scheme (projects with 2+ institutions)
        if len(ror_ids) >= 2:
            for pair in combinations(sorted(ror_ids), 2):
                collaboration_pairs[pair][funding_scheme] += 1

    print(f"  Found {projects_with_grant_id} projects with grant IDs")
    print(f"  Found {len(institution_projects)} unique institutions with ROR")
    print(f"  Found {len(collaboration_pairs)} collaboration pairs")
    print(f"  Found {len(funding_schemes)} funding schemes")

    return institution_projects, collaboration_pairs, sorted(funding_schemes), all_projects_list

def fetch_ror_data(ror_ids):
    """Fetch institution details from ROR API."""
    print(f"Fetching data for {len(ror_ids)} institutions from ROR API...")

    institutions = {}

    for i, ror_id in enumerate(ror_ids):
        try:
            url = f"{ROR_API}/{ror_id}"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()

                # Extract location from geonames_details
                lat, lng = None, None
                country = "Unknown"
                country_code = ""

                locations = data.get("locations", [])
                if locations:
                    geo = locations[0].get("geonames_details", {})
                    lat = geo.get("lat")
                    lng = geo.get("lng")
                    country = geo.get("country_name", "Unknown")
                    country_code = geo.get("country_code", "")

                # Extract name from names array (ROR API v2)
                # Look for the 'ror_display' type first, then any label
                name = "Unknown"
                names = data.get("names", [])
                for n in names:
                    if "ror_display" in n.get("types", []):
                        name = n.get("value", "Unknown")
                        break
                if name == "Unknown":
                    for n in names:
                        if "label" in n.get("types", []):
                            name = n.get("value", "Unknown")
                            break

                institutions[ror_id] = {
                    "name": name,
                    "lat": lat,
                    "lng": lng,
                    "country": country,
                    "country_code": country_code,
                    "ror_id": ror_id
                }

                if (i + 1) % 10 == 0:
                    print(f"  Processed {i + 1}/{len(ror_ids)} institutions...")
            else:
                print(f"  Warning: Could not fetch ROR {ror_id} (status {response.status_code})")

            time.sleep(0.2)  # Rate limiting

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching ROR {ror_id}: {e}")

    return institutions

def main():
    # Fetch NWO projects
    projects = fetch_nwo_projects(limit=500, reporting_years=[2022, 2023, 2024, 2025])

    if not projects:
        print("No projects fetched. Please check the API connection.")
        return

    print(f"Total projects fetched: {len(projects)}")

    # Extract collaborations
    institution_projects, collaboration_pairs, funding_schemes, all_projects_list = extract_collaborations(projects)

    if not institution_projects:
        print("No institutions with ROR IDs found in projects.")
        return

    # Fetch ROR data for all institutions
    institutions = fetch_ror_data(list(institution_projects.keys()))

    # Build output data
    output_institutions = []
    for ror_id, proj_list in institution_projects.items():
        if ror_id in institutions:
            inst = institutions[ror_id]
            if inst["lat"] is not None and inst["lng"] is not None:
                output_institutions.append({
                    "name": inst["name"],
                    "lat": inst["lat"],
                    "lng": inst["lng"],
                    "count": len(proj_list),
                    "projects": proj_list[:15],  # Limit to 15 projects per institution
                    "country": inst["country"],
                    "country_code": inst["country_code"],
                    "ror_id": ror_id
                })

    # Build collaboration links (top connections)
    output_links = []
    # Sort by total count across all schemes
    sorted_pairs = sorted(collaboration_pairs.items(), key=lambda x: -sum(x[1].values()))[:50]
    for (ror1, ror2), scheme_counts in sorted_pairs:
        if ror1 in institutions and ror2 in institutions:
            inst1, inst2 = institutions[ror1], institutions[ror2]
            if all([inst1["lat"], inst1["lng"], inst2["lat"], inst2["lng"]]):
                output_links.append({
                    "source": {"lat": inst1["lat"], "lng": inst1["lng"], "name": inst1["name"]},
                    "target": {"lat": inst2["lat"], "lng": inst2["lng"], "name": inst2["name"]},
                    "count": sum(scheme_counts.values()),  # Total count
                    "scheme_counts": dict(scheme_counts)   # Count per funding scheme
                })

    # Write output
    output = {
        "institutions": output_institutions,
        "links": output_links,
        "funding_schemes": funding_schemes,
        "all_projects": all_projects_list,
        "metadata": {
            "total_projects": len(all_projects_list),
            "total_institutions": len(output_institutions),
            "total_links": len(output_links),
            "total_funding_schemes": len(funding_schemes)
        }
    }

    with open("collaboration_data.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nData saved to collaboration_data.json")
    print(f"  - {len(output_institutions)} institutions with coordinates")
    print(f"  - {len(output_links)} collaboration links")

if __name__ == "__main__":
    main()
